//! Deterministic non-recursive condition evaluator.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use openfga_domain::{ContextValue, ParameterName};

use crate::{
    compiler::CompiledCondition,
    error::{EvaluationError, EvaluationErrorKind},
    ir::{BinaryOperator, Comprehension, Function, NodeId, NodeKind, UnaryOperator},
    types::{CancellationCheck, ConditionOutcome, EvaluationBudget, EvaluationContexts},
    value::{
        RuntimeValue, compare, convert_parameter, ip_in_cidr, parameter_value, parse_duration,
        parse_ip_address, parse_timestamp,
    },
};

#[derive(Debug)]
struct ComprehensionState {
    definition: Comprehension,
    items: Vec<(RuntimeValue, Option<RuntimeValue>)>,
    index: usize,
    accumulator: RuntimeValue,
}

#[derive(Debug)]
enum Frame {
    Eval(NodeId),
    Unary(UnaryOperator),
    Binary(BinaryOperator),
    LogicalAfterLeft {
        right: NodeId,
        and: bool,
        value_depth: usize,
    },
    LogicalAfterUnknown {
        unknown: BTreeSet<ParameterName>,
        and: bool,
        value_depth: usize,
    },
    LogicalAfterError {
        error: EvaluationError,
        and: bool,
        value_depth: usize,
    },
    Conditional {
        truthy: NodeId,
        falsy: NodeId,
    },
    Select {
        field: String,
        test: bool,
    },
    ListNext {
        ids: Vec<NodeId>,
        index: usize,
        values: Vec<RuntimeValue>,
    },
    ListAfter {
        ids: Vec<NodeId>,
        index: usize,
        values: Vec<RuntimeValue>,
    },
    MapNext {
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        values: Vec<(String, RuntimeValue)>,
    },
    MapAfterKey {
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        values: Vec<(String, RuntimeValue)>,
    },
    MapAfterValue {
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        key: String,
        values: Vec<(String, RuntimeValue)>,
    },
    CallNext {
        function: Function,
        ids: Vec<NodeId>,
        target: bool,
        index: usize,
        values: Vec<RuntimeValue>,
    },
    CallAfter {
        function: Function,
        ids: Vec<NodeId>,
        target: bool,
        index: usize,
        values: Vec<RuntimeValue>,
    },
    ComprehensionAfterRange(Comprehension),
    ComprehensionAfterInit {
        definition: Comprehension,
        items: Vec<(RuntimeValue, Option<RuntimeValue>)>,
    },
    ComprehensionNext(ComprehensionState),
    ComprehensionAfterCond(ComprehensionState),
    ComprehensionAfterStep(ComprehensionState),
    ScopeGuard,
}

pub(crate) fn evaluate(
    compiled: &CompiledCondition,
    contexts: EvaluationContexts<'_>,
    budget: EvaluationBudget,
    cancellation: &dyn CancellationCheck,
) -> Result<ConditionOutcome, EvaluationError> {
    let parameter_values = prepare_parameters(compiled, contexts, cancellation)?;
    let mut machine = Machine {
        compiled,
        cancellation,
        maximum_cost: budget.maximum_cost(),
        cost: 0,
        frames: vec![Frame::Eval(compiled.program.root)],
        values: Vec::new(),
        scopes: Vec::new(),
        parameter_values,
    };
    machine.run()
}

struct Machine<'a> {
    compiled: &'a CompiledCondition,
    cancellation: &'a dyn CancellationCheck,
    maximum_cost: u64,
    cost: u64,
    frames: Vec<Frame>,
    values: Vec<RuntimeValue>,
    scopes: Vec<BTreeMap<String, RuntimeValue>>,
    parameter_values: BTreeMap<ParameterName, RuntimeValue>,
}

impl Machine<'_> {
    fn run(&mut self) -> Result<ConditionOutcome, EvaluationError> {
        while let Some(frame) = self.frames.pop() {
            if let Err(error) = self.execute_frame(frame) {
                self.recover_logical_error(error)?;
            }
        }
        match self.values.pop() {
            Some(RuntimeValue::Bool(value)) if self.values.is_empty() => {
                Ok(ConditionOutcome::new(value, self.cost))
            }
            Some(RuntimeValue::Unknown(parameters)) if self.values.is_empty() => {
                Err(EvaluationError::missing(parameters.len()))
            }
            _ => Err(EvaluationError::new(
                EvaluationErrorKind::InvalidCompiledState,
            )),
        }
    }

    fn execute_frame(&mut self, frame: Frame) -> Result<(), EvaluationError> {
        match frame {
            Frame::Eval(id) => self.eval_node(id),
            Frame::Unary(operator) => self.apply_unary(operator),
            Frame::Binary(operator) => self.apply_binary(operator),
            Frame::LogicalAfterLeft {
                right,
                and,
                value_depth,
            } => self.logical_after_left(right, and, value_depth),
            Frame::LogicalAfterUnknown {
                unknown,
                and,
                value_depth: _,
            } => self.logical_after_unknown(unknown, and),
            Frame::LogicalAfterError {
                error,
                and,
                value_depth: _,
            } => self.logical_after_error(error, and),
            Frame::Conditional { truthy, falsy } => self.conditional(truthy, falsy),
            Frame::Select { field, test } => self.select(&field, test),
            Frame::ListNext { ids, index, values } => self.list_next(ids, index, values),
            Frame::ListAfter { ids, index, values } => self.list_after(ids, index, values),
            Frame::MapNext { ids, index, values } => self.map_next(ids, index, values),
            Frame::MapAfterKey { ids, index, values } => self.map_after_key(ids, index, values),
            Frame::MapAfterValue {
                ids,
                index,
                key,
                values,
            } => self.map_after_value(ids, index, key, values),
            Frame::CallNext {
                function,
                ids,
                target,
                index,
                values,
            } => self.call_next(function, ids, target, index, values),
            Frame::CallAfter {
                function,
                ids,
                target,
                index,
                mut values,
            } => {
                values.push(self.pop_value()?);
                self.frames.push(Frame::CallNext {
                    function,
                    ids,
                    target,
                    index: checked_increment(index)?,
                    values,
                });
                Ok(())
            }
            Frame::ComprehensionAfterRange(definition) => {
                self.comprehension_after_range(definition)
            }
            Frame::ComprehensionAfterInit { definition, items } => {
                self.comprehension_after_init(definition, items)
            }
            Frame::ComprehensionNext(state) => self.comprehension_next(state),
            Frame::ComprehensionAfterCond(state) => self.comprehension_after_cond(state),
            Frame::ComprehensionAfterStep(state) => self.comprehension_after_step(state),
            Frame::ScopeGuard => self.pop_scope(),
        }
    }

    fn list_after(
        &mut self,
        ids: Vec<NodeId>,
        index: usize,
        mut values: Vec<RuntimeValue>,
    ) -> Result<(), EvaluationError> {
        if values.len() >= self.compiled.runtime_collection_items {
            return Err(value_limit_exceeded());
        }
        values.push(self.pop_value()?);
        self.frames.push(Frame::ListNext {
            ids,
            index: checked_increment(index)?,
            values,
        });
        Ok(())
    }

    fn map_after_value(
        &mut self,
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        key: String,
        mut values: Vec<(String, RuntimeValue)>,
    ) -> Result<(), EvaluationError> {
        let value = self.pop_value()?;
        for (existing, _) in &values {
            self.check_cancellation()?;
            if existing == &key {
                return Err(invalid_value());
            }
        }
        if matches!(value, RuntimeValue::Unknown(_)) {
            self.values.push(value);
            return Ok(());
        }
        if values.len() >= self.compiled.runtime_collection_items
            || key.len() > self.compiled.runtime_value_bytes
        {
            return Err(value_limit_exceeded());
        }
        values.push((key, value));
        self.frames.push(Frame::MapNext {
            ids,
            index: checked_increment(index)?,
            values,
        });
        Ok(())
    }

    fn eval_node(&mut self, id: NodeId) -> Result<(), EvaluationError> {
        self.check_cancellation()?;
        let node = self
            .compiled
            .program
            .nodes
            .get(id)
            .ok_or_else(|| EvaluationError::new(EvaluationErrorKind::InvalidCompiledState))?
            .clone();
        match node.kind {
            NodeKind::Literal(value) => {
                self.validate_runtime_value(&value)?;
                self.values.push(value);
            }
            NodeKind::Parameter(name) => self.eval_parameter(&name)?,
            NodeKind::Local(name) => self.eval_local(&name)?,
            NodeKind::List(ids) => self.frames.push(Frame::ListNext {
                ids,
                index: 0,
                values: Vec::new(),
            }),
            NodeKind::Map(ids) => self.frames.push(Frame::MapNext {
                ids,
                index: 0,
                values: Vec::new(),
            }),
            NodeKind::Select {
                operand,
                field,
                test,
            } => {
                self.frames.push(Frame::Select { field, test });
                self.frames.push(Frame::Eval(operand));
            }
            NodeKind::Unary { operator, operand } => {
                self.frames.push(Frame::Unary(operator));
                self.frames.push(Frame::Eval(operand));
            }
            NodeKind::Binary {
                operator,
                left,
                right,
            } => {
                self.frames.push(Frame::Binary(operator));
                self.frames.push(Frame::Eval(right));
                self.frames.push(Frame::Eval(left));
            }
            NodeKind::LogicalAnd { left, right } => self.begin_logical(left, right, true),
            NodeKind::LogicalOr { left, right } => self.begin_logical(left, right, false),
            NodeKind::Conditional {
                condition,
                truthy,
                falsy,
            } => {
                self.frames.push(Frame::Conditional { truthy, falsy });
                self.frames.push(Frame::Eval(condition));
            }
            NodeKind::Call {
                function,
                target,
                arguments,
            } => {
                let mut ids = Vec::with_capacity(
                    arguments
                        .len()
                        .saturating_add(usize::from(target.is_some())),
                );
                if let Some(target) = target {
                    ids.push(target);
                }
                ids.extend(arguments);
                self.frames.push(Frame::CallNext {
                    function,
                    ids,
                    target: target.is_some(),
                    index: 0,
                    values: Vec::new(),
                });
            }
            NodeKind::Comprehension(definition) => {
                let range = definition.iter_range;
                self.frames.push(Frame::ComprehensionAfterRange(definition));
                self.frames.push(Frame::Eval(range));
            }
        }
        Ok(())
    }

    fn eval_parameter(&mut self, name: &ParameterName) -> Result<(), EvaluationError> {
        self.charge()?;
        if !self.compiled.parameters().contains_key(name) {
            return Err(EvaluationError::new(
                EvaluationErrorKind::InvalidCompiledState,
            ));
        }
        match self.parameter_values.get(name) {
            Some(value) => self.values.push(value.clone()),
            None => self
                .values
                .push(RuntimeValue::Unknown(BTreeSet::from([name.clone()]))),
        }
        Ok(())
    }

    fn eval_local(&mut self, name: &str) -> Result<(), EvaluationError> {
        self.charge()?;
        let value = self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .cloned()
            .ok_or_else(|| EvaluationError::new(EvaluationErrorKind::InvalidCompiledState))?;
        self.values.push(value);
        Ok(())
    }

    fn begin_logical(&mut self, left: NodeId, right: NodeId, and: bool) {
        self.frames.push(Frame::LogicalAfterLeft {
            right,
            and,
            value_depth: self.values.len(),
        });
        self.frames.push(Frame::Eval(left));
    }

    fn logical_after_left(
        &mut self,
        right: NodeId,
        and: bool,
        value_depth: usize,
    ) -> Result<(), EvaluationError> {
        match self.pop_value()? {
            RuntimeValue::Bool(value) if value != and => {
                self.values.push(RuntimeValue::Bool(value));
            }
            RuntimeValue::Bool(_) => self.frames.push(Frame::Eval(right)),
            RuntimeValue::Unknown(unknown) => {
                self.frames.push(Frame::LogicalAfterUnknown {
                    unknown,
                    and,
                    value_depth,
                });
                self.frames.push(Frame::Eval(right));
            }
            _ => return Err(type_mismatch()),
        }
        Ok(())
    }

    fn logical_after_unknown(
        &mut self,
        mut unknown: BTreeSet<ParameterName>,
        and: bool,
    ) -> Result<(), EvaluationError> {
        match self.pop_value()? {
            RuntimeValue::Bool(value) if value != and => {
                self.values.push(RuntimeValue::Bool(value));
            }
            RuntimeValue::Bool(_) => self.values.push(RuntimeValue::Unknown(unknown)),
            RuntimeValue::Unknown(other) => {
                unknown.extend(other);
                self.values.push(RuntimeValue::Unknown(unknown));
            }
            _ => return Err(type_mismatch()),
        }
        Ok(())
    }

    fn logical_after_error(
        &mut self,
        error: EvaluationError,
        and: bool,
    ) -> Result<(), EvaluationError> {
        match self.pop_value()? {
            RuntimeValue::Bool(value) if value != and => {
                self.values.push(RuntimeValue::Bool(value));
                Ok(())
            }
            RuntimeValue::Bool(_) | RuntimeValue::Unknown(_) => Err(error),
            _ => Err(type_mismatch()),
        }
    }

    fn recover_logical_error(&mut self, mut error: EvaluationError) -> Result<(), EvaluationError> {
        if !is_suppressible(&error) {
            return Err(error);
        }
        while let Some(frame) = self.frames.pop() {
            match frame {
                Frame::LogicalAfterLeft {
                    right,
                    and,
                    value_depth,
                } => {
                    self.values.truncate(value_depth);
                    self.frames.push(Frame::LogicalAfterError {
                        error,
                        and,
                        value_depth,
                    });
                    self.frames.push(Frame::Eval(right));
                    return Ok(());
                }
                Frame::LogicalAfterError {
                    error: original,
                    value_depth,
                    ..
                } => {
                    self.values.truncate(value_depth);
                    error = original;
                }
                Frame::LogicalAfterUnknown { value_depth, .. } => {
                    self.values.truncate(value_depth);
                }
                Frame::ScopeGuard => self.pop_scope()?,
                _ => {}
            }
        }
        Err(error)
    }

    fn conditional(&mut self, truthy: NodeId, falsy: NodeId) -> Result<(), EvaluationError> {
        match self.pop_value()? {
            RuntimeValue::Bool(true) => self.frames.push(Frame::Eval(truthy)),
            RuntimeValue::Bool(false) => self.frames.push(Frame::Eval(falsy)),
            RuntimeValue::Unknown(unknown) => self.values.push(RuntimeValue::Unknown(unknown)),
            _ => return Err(type_mismatch()),
        }
        Ok(())
    }

    fn apply_unary(&mut self, operator: UnaryOperator) -> Result<(), EvaluationError> {
        self.charge()?;
        let value = self.pop_value()?;
        let result = match (operator, value) {
            (UnaryOperator::NotStrictlyFalse, RuntimeValue::Unknown(_)) => RuntimeValue::Bool(true),
            (_, RuntimeValue::Unknown(unknown)) => RuntimeValue::Unknown(unknown),
            (UnaryOperator::Not, RuntimeValue::Bool(value)) => RuntimeValue::Bool(!value),
            (UnaryOperator::NotStrictlyFalse, RuntimeValue::Bool(value)) => {
                RuntimeValue::Bool(value)
            }
            (UnaryOperator::Negate, RuntimeValue::Int(value)) => {
                RuntimeValue::Int(value.checked_neg().ok_or_else(arithmetic)?)
            }
            (UnaryOperator::Negate, RuntimeValue::Double(value)) => RuntimeValue::Double(-value),
            _ => return Err(type_mismatch()),
        };
        self.values.push(result);
        Ok(())
    }

    fn apply_binary(&mut self, operator: BinaryOperator) -> Result<(), EvaluationError> {
        let right = self.pop_value()?;
        let left = self.pop_value()?;
        self.charge_binary(operator, &left, &right)?;
        if let Some(unknown) = merge_unknown(&left, &right) {
            self.values.push(RuntimeValue::Unknown(unknown));
            return Ok(());
        }
        let result = match operator {
            BinaryOperator::Equal => RuntimeValue::Bool(self.values_equal(&left, &right)?),
            BinaryOperator::NotEqual => RuntimeValue::Bool(!self.values_equal(&left, &right)?),
            BinaryOperator::Greater => ordered(&left, &right, |order| order == Ordering::Greater)?,
            BinaryOperator::GreaterEqual => {
                ordered(&left, &right, |order| order != Ordering::Less)?
            }
            BinaryOperator::Less => ordered(&left, &right, |order| order == Ordering::Less)?,
            BinaryOperator::LessEqual => {
                ordered(&left, &right, |order| order != Ordering::Greater)?
            }
            BinaryOperator::Add => self.add(left, right)?,
            BinaryOperator::Subtract => subtract(left, right)?,
            BinaryOperator::Multiply => multiply(left, right)?,
            BinaryOperator::Divide => divide(left, right)?,
            BinaryOperator::Modulo => modulo(left, right)?,
            BinaryOperator::In => self.membership(left, right)?,
            BinaryOperator::Index => self.index(left, right)?,
        };
        self.values.push(result);
        Ok(())
    }

    fn membership(
        &mut self,
        needle: RuntimeValue,
        haystack: RuntimeValue,
    ) -> Result<RuntimeValue, EvaluationError> {
        match haystack {
            RuntimeValue::List(values) => {
                for value in values {
                    self.check_cancellation()?;
                    if self.values_equal(&value, &needle)? {
                        return Ok(RuntimeValue::Bool(true));
                    }
                }
                Ok(RuntimeValue::Bool(false))
            }
            RuntimeValue::Map(values) => {
                let RuntimeValue::String(needle) = needle else {
                    return Err(type_mismatch());
                };
                self.check_cancellation()?;
                Ok(RuntimeValue::Bool(
                    values
                        .binary_search_by(|(key, _)| key.as_str().cmp(needle.as_str()))
                        .is_ok(),
                ))
            }
            _ => Err(type_mismatch()),
        }
    }

    fn add(
        &mut self,
        left: RuntimeValue,
        right: RuntimeValue,
    ) -> Result<RuntimeValue, EvaluationError> {
        match (left, right) {
            (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left
                .checked_add(right)
                .map(RuntimeValue::Int)
                .ok_or_else(arithmetic),
            (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left
                .checked_add(right)
                .map(RuntimeValue::Uint)
                .ok_or_else(arithmetic),
            (RuntimeValue::Double(left), RuntimeValue::Double(right)) => finite(left + right),
            (RuntimeValue::String(mut left), RuntimeValue::String(right)) => {
                bound_sum(left.len(), right.len(), self.compiled.runtime_value_bytes)?;
                self.check_cancellation()?;
                left.push_str(&right);
                Ok(RuntimeValue::String(left))
            }
            (RuntimeValue::Bytes(mut left), RuntimeValue::Bytes(right)) => {
                bound_sum(left.len(), right.len(), self.compiled.runtime_value_bytes)?;
                self.check_cancellation()?;
                left.extend(right);
                Ok(RuntimeValue::Bytes(left))
            }
            (RuntimeValue::List(mut left), RuntimeValue::List(right)) => {
                bound_sum(
                    left.len(),
                    right.len(),
                    self.compiled.runtime_collection_items,
                )?;
                for value in right {
                    self.check_cancellation()?;
                    left.push(value);
                }
                Ok(RuntimeValue::List(left))
            }
            _ => Err(type_mismatch()),
        }
    }

    fn index(
        &mut self,
        collection: RuntimeValue,
        index: RuntimeValue,
    ) -> Result<RuntimeValue, EvaluationError> {
        match (collection, index) {
            (RuntimeValue::List(values), RuntimeValue::Int(index)) => usize::try_from(index)
                .ok()
                .and_then(|index| values.get(index).cloned())
                .ok_or_else(invalid_value),
            (RuntimeValue::List(values), RuntimeValue::Uint(index)) => usize::try_from(index)
                .ok()
                .and_then(|index| values.get(index).cloned())
                .ok_or_else(invalid_value),
            (RuntimeValue::Map(values), RuntimeValue::String(index)) => {
                self.check_cancellation()?;
                let position = values
                    .binary_search_by(|(key, _)| key.as_str().cmp(index.as_str()))
                    .map_err(|_| invalid_value())?;
                values
                    .get(position)
                    .map(|(_, value)| value.clone())
                    .ok_or_else(invalid_state)
            }
            _ => Err(type_mismatch()),
        }
    }

    fn select(&mut self, field: &str, test: bool) -> Result<(), EvaluationError> {
        self.charge()?;
        match self.pop_value()? {
            RuntimeValue::Unknown(unknown) => self.values.push(RuntimeValue::Unknown(unknown)),
            RuntimeValue::Map(values) => {
                self.check_cancellation()?;
                let selected = values
                    .binary_search_by(|(key, _)| key.as_str().cmp(field))
                    .ok()
                    .and_then(|position| values.get(position))
                    .map(|(_, value)| value.clone());
                if test {
                    self.values.push(RuntimeValue::Bool(selected.is_some()));
                } else {
                    self.values.push(selected.ok_or_else(invalid_value)?);
                }
            }
            _ => return Err(type_mismatch()),
        }
        Ok(())
    }

    fn list_next(
        &mut self,
        ids: Vec<NodeId>,
        index: usize,
        values: Vec<RuntimeValue>,
    ) -> Result<(), EvaluationError> {
        if let Some(id) = ids.get(index).copied() {
            self.check_cancellation()?;
            self.frames.push(Frame::ListAfter { ids, index, values });
            self.frames.push(Frame::Eval(id));
        } else {
            self.charge_units(10)?;
            if let Some(unknown) = collect_top_level_unknowns(values.iter()) {
                self.values.push(RuntimeValue::Unknown(unknown));
            } else {
                self.values.push(RuntimeValue::List(values));
            }
        }
        Ok(())
    }

    fn map_next(
        &mut self,
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        mut values: Vec<(String, RuntimeValue)>,
    ) -> Result<(), EvaluationError> {
        if let Some((key, _)) = ids.get(index).copied() {
            self.check_cancellation()?;
            self.frames.push(Frame::MapAfterKey { ids, index, values });
            self.frames.push(Frame::Eval(key));
        } else {
            self.charge_units(30)?;
            if let Some(unknown) = collect_top_level_unknowns(values.iter().map(|(_, value)| value))
            {
                self.values.push(RuntimeValue::Unknown(unknown));
            } else {
                values.sort_by(|(left, _), (right, _)| left.cmp(right));
                self.values.push(RuntimeValue::Map(values));
            }
        }
        Ok(())
    }

    fn map_after_key(
        &mut self,
        ids: Vec<(NodeId, NodeId)>,
        index: usize,
        values: Vec<(String, RuntimeValue)>,
    ) -> Result<(), EvaluationError> {
        let key = match self.pop_value()? {
            RuntimeValue::String(key) => key,
            RuntimeValue::Unknown(unknown) => {
                self.values.push(RuntimeValue::Unknown(unknown));
                return Ok(());
            }
            _ => return Err(type_mismatch()),
        };
        let value = ids
            .get(index)
            .map(|(_, value)| *value)
            .ok_or_else(invalid_state)?;
        self.frames.push(Frame::MapAfterValue {
            ids,
            index,
            key,
            values,
        });
        self.frames.push(Frame::Eval(value));
        Ok(())
    }

    fn call_next(
        &mut self,
        function: Function,
        ids: Vec<NodeId>,
        target: bool,
        index: usize,
        values: Vec<RuntimeValue>,
    ) -> Result<(), EvaluationError> {
        if let Some(id) = ids.get(index).copied() {
            self.frames.push(Frame::CallAfter {
                function,
                ids,
                target,
                index,
                values,
            });
            self.frames.push(Frame::Eval(id));
        } else {
            self.charge_function(function, target, &values)?;
            self.values.push(apply_function(function, target, values)?);
        }
        Ok(())
    }

    fn comprehension_after_range(
        &mut self,
        definition: Comprehension,
    ) -> Result<(), EvaluationError> {
        let range = self.pop_value()?;
        let items = match range {
            RuntimeValue::List(values) => values.into_iter().map(|value| (value, None)).collect(),
            RuntimeValue::Map(values) => values
                .into_iter()
                .map(|(key, value)| (RuntimeValue::String(key), Some(value)))
                .collect(),
            RuntimeValue::Unknown(unknown) => {
                self.values.push(RuntimeValue::Unknown(unknown));
                return Ok(());
            }
            _ => return Err(type_mismatch()),
        };
        let init = definition.accu_init;
        self.frames
            .push(Frame::ComprehensionAfterInit { definition, items });
        self.frames.push(Frame::Eval(init));
        Ok(())
    }

    fn comprehension_after_init(
        &mut self,
        definition: Comprehension,
        items: Vec<(RuntimeValue, Option<RuntimeValue>)>,
    ) -> Result<(), EvaluationError> {
        let accumulator = self.pop_value()?;
        self.scopes.push(BTreeMap::new());
        self.frames.push(Frame::ScopeGuard);
        self.frames
            .push(Frame::ComprehensionNext(ComprehensionState {
                definition,
                items,
                index: 0,
                accumulator,
            }));
        Ok(())
    }

    fn comprehension_next(&mut self, state: ComprehensionState) -> Result<(), EvaluationError> {
        if state.index >= state.items.len() {
            let result = state.definition.result;
            self.set_local(&state.definition.accu_var, state.accumulator)?;
            self.frames.push(Frame::Eval(result));
            return Ok(());
        }
        self.check_cancellation()?;
        let (first, second) = state
            .items
            .get(state.index)
            .cloned()
            .ok_or_else(invalid_state)?;
        self.set_local(&state.definition.iter_var, first)?;
        if let (Some(name), Some(value)) = (&state.definition.iter_var2, second) {
            self.set_local(name, value)?;
        }
        self.set_local(&state.definition.accu_var, state.accumulator.clone())?;
        let condition = state.definition.loop_cond;
        self.frames.push(Frame::ComprehensionAfterCond(state));
        self.frames.push(Frame::Eval(condition));
        Ok(())
    }

    fn comprehension_after_cond(
        &mut self,
        state: ComprehensionState,
    ) -> Result<(), EvaluationError> {
        match self.pop_value()? {
            RuntimeValue::Bool(true) => {
                let step = state.definition.loop_step;
                self.frames.push(Frame::ComprehensionAfterStep(state));
                self.frames.push(Frame::Eval(step));
            }
            RuntimeValue::Bool(false) => {
                let result = state.definition.result;
                self.set_local(&state.definition.accu_var, state.accumulator)?;
                self.frames.push(Frame::Eval(result));
            }
            RuntimeValue::Unknown(unknown) => {
                self.values.push(RuntimeValue::Unknown(unknown));
            }
            _ => return Err(type_mismatch()),
        }
        Ok(())
    }

    fn comprehension_after_step(
        &mut self,
        mut state: ComprehensionState,
    ) -> Result<(), EvaluationError> {
        state.accumulator = self.pop_value()?;
        state.index = checked_increment(state.index)?;
        self.frames.push(Frame::ComprehensionNext(state));
        Ok(())
    }

    fn pop_scope(&mut self) -> Result<(), EvaluationError> {
        self.scopes.pop().ok_or_else(invalid_state).map(|_| ())
    }

    fn set_local(&mut self, name: &str, value: RuntimeValue) -> Result<(), EvaluationError> {
        let scope = self.scopes.last_mut().ok_or_else(invalid_state)?;
        scope.insert(name.to_owned(), value);
        Ok(())
    }

    fn charge(&mut self) -> Result<(), EvaluationError> {
        self.charge_units(1)
    }

    fn charge_units(&mut self, units: u64) -> Result<(), EvaluationError> {
        if self.cancellation.is_cancelled() {
            return Err(EvaluationError::new(EvaluationErrorKind::Cancelled));
        }
        self.cost = self.cost.checked_add(units).ok_or_else(cost_exceeded)?;
        if self.cost > self.maximum_cost {
            return Err(cost_exceeded());
        }
        Ok(())
    }

    fn charge_binary(
        &mut self,
        operator: BinaryOperator,
        left: &RuntimeValue,
        right: &RuntimeValue,
    ) -> Result<(), EvaluationError> {
        let units = match operator {
            BinaryOperator::Equal | BinaryOperator::NotEqual => {
                traversal_cost(runtime_size(left).min(runtime_size(right)))?
            }
            BinaryOperator::Greater
            | BinaryOperator::GreaterEqual
            | BinaryOperator::Less
            | BinaryOperator::LessEqual
                if matches!(left, RuntimeValue::String(_) | RuntimeValue::Bytes(_)) =>
            {
                traversal_cost(runtime_size(left).min(runtime_size(right)))?
            }
            BinaryOperator::Add
                if matches!(left, RuntimeValue::String(_) | RuntimeValue::Bytes(_)) =>
            {
                traversal_cost(
                    runtime_size(left)
                        .checked_add(runtime_size(right))
                        .ok_or_else(cost_exceeded)?,
                )?
            }
            BinaryOperator::In if matches!(right, RuntimeValue::List(_)) => runtime_size(right),
            _ => 1,
        };
        self.charge_units(units)
    }

    fn charge_function(
        &mut self,
        function: Function,
        target: bool,
        values: &[RuntimeValue],
    ) -> Result<(), EvaluationError> {
        let receiver = if target { values.first() } else { None };
        let first_argument = values.get(usize::from(target));
        let units = match (function, receiver, first_argument) {
            (
                Function::Contains,
                Some(RuntimeValue::String(receiver)),
                Some(RuntimeValue::String(argument)),
            ) => traversal_cost(usize_to_u64(receiver.chars().count()))?
                .checked_mul(traversal_cost(usize_to_u64(argument.chars().count()))?)
                .ok_or_else(cost_exceeded)?,
            (
                Function::StartsWith | Function::EndsWith,
                Some(RuntimeValue::String(_)),
                Some(RuntimeValue::String(argument)),
            ) => traversal_cost(usize_to_u64(argument.chars().count()))?,
            (Function::Bytes, None, Some(RuntimeValue::String(value))) => {
                traversal_cost(usize_to_u64(value.chars().count()))?
            }
            (Function::String, None, Some(RuntimeValue::Bytes(value))) => {
                traversal_cost(usize_to_u64(value.len()))?
            }
            _ => 1,
        };
        self.charge_units(units)
    }

    fn check_cancellation(&self) -> Result<(), EvaluationError> {
        if self.cancellation.is_cancelled() {
            Err(EvaluationError::new(EvaluationErrorKind::Cancelled))
        } else {
            Ok(())
        }
    }

    fn validate_runtime_value(&self, value: &RuntimeValue) -> Result<(), EvaluationError> {
        let within_limit = match value {
            RuntimeValue::String(value) => value.len() <= self.compiled.runtime_value_bytes,
            RuntimeValue::Bytes(value) => value.len() <= self.compiled.runtime_value_bytes,
            RuntimeValue::List(value) => value.len() <= self.compiled.runtime_collection_items,
            RuntimeValue::Map(value) => {
                value.len() <= self.compiled.runtime_collection_items
                    && value
                        .iter()
                        .all(|(key, _)| key.len() <= self.compiled.runtime_value_bytes)
            }
            _ => true,
        };
        if within_limit {
            Ok(())
        } else {
            Err(value_limit_exceeded())
        }
    }

    fn pop_value(&mut self) -> Result<RuntimeValue, EvaluationError> {
        self.values.pop().ok_or_else(invalid_state)
    }

    fn values_equal(
        &mut self,
        left: &RuntimeValue,
        right: &RuntimeValue,
    ) -> Result<bool, EvaluationError> {
        let mut stack = vec![(left, right)];
        while let Some((left, right)) = stack.pop() {
            match (left, right) {
                (RuntimeValue::List(left), RuntimeValue::List(right)) => {
                    if left.len() != right.len() {
                        return Ok(false);
                    }
                    for pair in left.iter().zip(right) {
                        self.check_cancellation()?;
                        stack.push(pair);
                    }
                }
                (RuntimeValue::Map(left), RuntimeValue::Map(right)) => {
                    if left.len() != right.len() {
                        return Ok(false);
                    }
                    for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right)
                    {
                        self.check_cancellation()?;
                        if left_key != right_key {
                            return Ok(false);
                        }
                        stack.push((left_value, right_value));
                    }
                }
                _ if !scalar_equal(left, right) => return Ok(false),
                _ => {}
            }
        }
        Ok(true)
    }
}

// CEL double equality is exact; all runtime doubles are finite and canonicalized at the boundary.
#[allow(clippy::float_cmp)]
fn scalar_equal(left: &RuntimeValue, right: &RuntimeValue) -> bool {
    match (left, right) {
        (RuntimeValue::Null, RuntimeValue::Null) => true,
        (RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => left == right,
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left == right,
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left == right,
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => left == right,
        (
            RuntimeValue::Int(_) | RuntimeValue::Uint(_) | RuntimeValue::Double(_),
            RuntimeValue::Int(_) | RuntimeValue::Uint(_) | RuntimeValue::Double(_),
        ) => compare(left, right).is_some_and(Ordering::is_eq),
        (RuntimeValue::String(left), RuntimeValue::String(right)) => left == right,
        (RuntimeValue::Bytes(left), RuntimeValue::Bytes(right)) => left == right,
        (RuntimeValue::Duration(left), RuntimeValue::Duration(right)) => left == right,
        (RuntimeValue::Timestamp(left), RuntimeValue::Timestamp(right)) => left == right,
        (RuntimeValue::IpAddress(left), RuntimeValue::IpAddress(right)) => left == right,
        _ => false,
    }
}

fn runtime_size(value: &RuntimeValue) -> u64 {
    let size = match value {
        RuntimeValue::String(value) => value.chars().count(),
        RuntimeValue::Bytes(value) => value.len(),
        RuntimeValue::List(value) => value.len(),
        RuntimeValue::Map(value) => value.len(),
        _ => 1,
    };
    usize_to_u64(size)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn traversal_cost(size: u64) -> Result<u64, EvaluationError> {
    size.checked_add(9)
        .map(|adjusted| adjusted / 10)
        .ok_or_else(cost_exceeded)
}

fn collect_top_level_unknowns<'a>(
    values: impl IntoIterator<Item = &'a RuntimeValue>,
) -> Option<BTreeSet<ParameterName>> {
    let mut unknown = BTreeSet::new();
    for value in values {
        if let RuntimeValue::Unknown(parameters) = value {
            unknown.extend(parameters.iter().cloned());
        }
    }
    (!unknown.is_empty()).then_some(unknown)
}

fn prepare_parameters(
    compiled: &CompiledCondition,
    contexts: EvaluationContexts<'_>,
    cancellation: &dyn CancellationCheck,
) -> Result<BTreeMap<ParameterName, RuntimeValue>, EvaluationError> {
    let mut values = BTreeMap::new();
    let mut missing = 0_usize;
    for (name, parameter_type) in compiled.parameters() {
        let Some(value) = parameter_value(name, contexts.request, contexts.tuple) else {
            missing = missing.checked_add(1).ok_or_else(invalid_state)?;
            continue;
        };
        charge_context_value(
            value,
            cancellation,
            compiled.runtime_value_bytes,
            compiled.runtime_collection_items,
        )?;
        values.insert(name.clone(), convert_parameter(value, parameter_type)?);
    }
    if missing != 0 {
        return Err(EvaluationError::missing(missing));
    }
    Ok(values)
}

fn charge_context_value(
    root: &ContextValue,
    cancellation: &dyn CancellationCheck,
    maximum_value_bytes: usize,
    maximum_collection_items: usize,
) -> Result<(), EvaluationError> {
    let mut stack = vec![root];
    while let Some(value) = stack.pop() {
        if cancellation.is_cancelled() {
            return Err(EvaluationError::new(EvaluationErrorKind::Cancelled));
        }
        match value {
            ContextValue::String(value) if value.as_str().len() > maximum_value_bytes => {
                return Err(value_limit_exceeded());
            }
            ContextValue::Bytes(value) if value.as_slice().len() > maximum_value_bytes => {
                return Err(value_limit_exceeded());
            }
            ContextValue::List(values) => {
                if values.as_slice().len() > maximum_collection_items {
                    return Err(value_limit_exceeded());
                }
                stack.extend(values.as_slice());
            }
            ContextValue::Map(values) => {
                let mut entries = 0_usize;
                for (key, value) in values {
                    entries = entries.checked_add(1).ok_or_else(value_limit_exceeded)?;
                    if entries > maximum_collection_items
                        || key.as_str().len() > maximum_value_bytes
                    {
                        return Err(value_limit_exceeded());
                    }
                    stack.push(value);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn merge_unknown(left: &RuntimeValue, right: &RuntimeValue) -> Option<BTreeSet<ParameterName>> {
    let mut unknown = BTreeSet::new();
    if let RuntimeValue::Unknown(values) = left {
        unknown.extend(values.iter().cloned());
    }
    if let RuntimeValue::Unknown(values) = right {
        unknown.extend(values.iter().cloned());
    }
    (!unknown.is_empty()).then_some(unknown)
}

fn ordered(
    left: &RuntimeValue,
    right: &RuntimeValue,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<RuntimeValue, EvaluationError> {
    compare(left, right)
        .map(|order| RuntimeValue::Bool(predicate(order)))
        .ok_or_else(type_mismatch)
}

fn subtract(left: RuntimeValue, right: RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match (left, right) {
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left
            .checked_sub(right)
            .map(RuntimeValue::Int)
            .ok_or_else(arithmetic),
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left
            .checked_sub(right)
            .map(RuntimeValue::Uint)
            .ok_or_else(arithmetic),
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => finite(left - right),
        _ => Err(type_mismatch()),
    }
}

fn multiply(left: RuntimeValue, right: RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match (left, right) {
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left
            .checked_mul(right)
            .map(RuntimeValue::Int)
            .ok_or_else(arithmetic),
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left
            .checked_mul(right)
            .map(RuntimeValue::Uint)
            .ok_or_else(arithmetic),
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => finite(left * right),
        _ => Err(type_mismatch()),
    }
}

fn divide(left: RuntimeValue, right: RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match (left, right) {
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left
            .checked_div(right)
            .map(RuntimeValue::Int)
            .ok_or_else(arithmetic),
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left
            .checked_div(right)
            .map(RuntimeValue::Uint)
            .ok_or_else(arithmetic),
        (RuntimeValue::Double(_), RuntimeValue::Double(0.0)) => Err(arithmetic()),
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => finite(left / right),
        _ => Err(type_mismatch()),
    }
}

fn modulo(left: RuntimeValue, right: RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match (left, right) {
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left
            .checked_rem(right)
            .map(RuntimeValue::Int)
            .ok_or_else(arithmetic),
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left
            .checked_rem(right)
            .map(RuntimeValue::Uint)
            .ok_or_else(arithmetic),
        (RuntimeValue::Double(_), RuntimeValue::Double(0.0)) => Err(arithmetic()),
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => finite(left % right),
        _ => Err(type_mismatch()),
    }
}

fn finite(value: f64) -> Result<RuntimeValue, EvaluationError> {
    value
        .is_finite()
        .then_some(RuntimeValue::Double(value))
        .ok_or_else(arithmetic)
}

fn apply_function(
    function: Function,
    target: bool,
    mut values: Vec<RuntimeValue>,
) -> Result<RuntimeValue, EvaluationError> {
    if values
        .iter()
        .any(|value| matches!(value, RuntimeValue::Unknown(_)))
    {
        let mut unknown = BTreeSet::new();
        for value in values {
            if let RuntimeValue::Unknown(parameters) = value {
                unknown.extend(parameters);
            }
        }
        return Ok(RuntimeValue::Unknown(unknown));
    }
    let receiver = if target {
        Some(remove_first(&mut values)?)
    } else {
        None
    };
    match (function, receiver, values.as_slice()) {
        (Function::Duration, None, [RuntimeValue::String(value)]) => {
            parse_duration(value).map(RuntimeValue::Duration)
        }
        (Function::Timestamp, None, [RuntimeValue::String(value)]) => {
            parse_timestamp(value).map(RuntimeValue::Timestamp)
        }
        (Function::IpAddress, None, [RuntimeValue::String(value)]) => {
            parse_ip_address(value).map(RuntimeValue::IpAddress)
        }
        (Function::Bytes, None, [RuntimeValue::String(value)]) => {
            Ok(RuntimeValue::Bytes(value.as_bytes().to_vec()))
        }
        (Function::Size, Some(value), []) => size(value),
        (Function::Size, None, [value]) => size(value.clone()),
        (
            Function::Contains,
            Some(RuntimeValue::String(receiver)),
            [RuntimeValue::String(argument)],
        ) => Ok(RuntimeValue::Bool(receiver.contains(argument))),
        (
            Function::StartsWith,
            Some(RuntimeValue::String(receiver)),
            [RuntimeValue::String(argument)],
        ) => Ok(RuntimeValue::Bool(receiver.starts_with(argument))),
        (
            Function::EndsWith,
            Some(RuntimeValue::String(receiver)),
            [RuntimeValue::String(argument)],
        ) => Ok(RuntimeValue::Bool(receiver.ends_with(argument))),
        (
            Function::InCidr,
            Some(RuntimeValue::IpAddress(address)),
            [RuntimeValue::String(cidr)],
        ) => ip_in_cidr(address, cidr).map(RuntimeValue::Bool),
        (Function::Int, None, [value]) => cast_int(value),
        (Function::Uint, None, [value]) => cast_uint(value),
        (Function::Double, None, [value]) => cast_double(value),
        (Function::String, None, [value]) => cast_string(value),
        _ => Err(type_mismatch()),
    }
}

fn remove_first(values: &mut Vec<RuntimeValue>) -> Result<RuntimeValue, EvaluationError> {
    if values.is_empty() {
        Err(invalid_state())
    } else {
        Ok(values.remove(0))
    }
}

fn size(value: RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    let length = match value {
        RuntimeValue::String(value) => value.chars().count(),
        RuntimeValue::Bytes(value) => value.len(),
        RuntimeValue::List(value) => value.len(),
        RuntimeValue::Map(value) => value.len(),
        _ => return Err(type_mismatch()),
    };
    i64::try_from(length)
        .map(RuntimeValue::Int)
        .map_err(|_| arithmetic())
}

fn cast_int(value: &RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match value {
        RuntimeValue::Int(value) => Ok(RuntimeValue::Int(*value)),
        RuntimeValue::Uint(value) => i64::try_from(*value)
            .map(RuntimeValue::Int)
            .map_err(|_| invalid_value()),
        RuntimeValue::Double(value) => exact_double_to_int(*value).map(RuntimeValue::Int),
        RuntimeValue::String(value) => value
            .parse()
            .map(RuntimeValue::Int)
            .map_err(|_| invalid_value()),
        _ => Err(type_mismatch()),
    }
}

fn cast_uint(value: &RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    match value {
        RuntimeValue::Uint(value) => Ok(RuntimeValue::Uint(*value)),
        RuntimeValue::Int(value) => u64::try_from(*value)
            .map(RuntimeValue::Uint)
            .map_err(|_| invalid_value()),
        RuntimeValue::Double(value) => exact_double_to_uint(*value).map(RuntimeValue::Uint),
        RuntimeValue::String(value) => value
            .parse()
            .map(RuntimeValue::Uint)
            .map_err(|_| invalid_value()),
        _ => Err(type_mismatch()),
    }
}

fn cast_double(value: &RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    let value = match value {
        RuntimeValue::Double(value) => *value,
        RuntimeValue::Int(value) => value.to_string().parse().map_err(|_| invalid_value())?,
        RuntimeValue::Uint(value) => value.to_string().parse().map_err(|_| invalid_value())?,
        RuntimeValue::String(value) => value.parse().map_err(|_| invalid_value())?,
        _ => return Err(type_mismatch()),
    };
    finite(value)
}

fn cast_string(value: &RuntimeValue) -> Result<RuntimeValue, EvaluationError> {
    let value = match value {
        RuntimeValue::String(value) => value.clone(),
        RuntimeValue::Int(value) => value.to_string(),
        RuntimeValue::Uint(value) => value.to_string(),
        RuntimeValue::Double(value) => value.to_string(),
        RuntimeValue::Bool(value) => value.to_string(),
        RuntimeValue::Bytes(value) => {
            String::from_utf8(value.clone()).map_err(|_| invalid_value())?
        }
        _ => return Err(type_mismatch()),
    };
    Ok(RuntimeValue::String(value))
}

fn exact_double_to_int(value: f64) -> Result<i64, EvaluationError> {
    if !value.is_finite() || value.fract() != 0.0 {
        return Err(invalid_value());
    }
    value.to_string().parse().map_err(|_| invalid_value())
}

fn exact_double_to_uint(value: f64) -> Result<u64, EvaluationError> {
    if !value.is_finite() || value.fract() != 0.0 || value.is_sign_negative() {
        return Err(invalid_value());
    }
    value.to_string().parse().map_err(|_| invalid_value())
}

const fn type_mismatch() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::TypeMismatch)
}
const fn arithmetic() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::Arithmetic)
}
const fn invalid_value() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::InvalidValue)
}
const fn invalid_state() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::InvalidCompiledState)
}
const fn cost_exceeded() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::CostExceeded)
}

fn is_suppressible(error: &EvaluationError) -> bool {
    matches!(
        error.kind(),
        EvaluationErrorKind::TypeMismatch
            | EvaluationErrorKind::Arithmetic
            | EvaluationErrorKind::InvalidValue
    )
}

fn bound_sum(left: usize, right: usize, maximum: usize) -> Result<(), EvaluationError> {
    let total = left.checked_add(right).ok_or_else(value_limit_exceeded)?;
    if total > maximum {
        Err(value_limit_exceeded())
    } else {
        Ok(())
    }
}

const fn value_limit_exceeded() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::ValueLimitExceeded)
}

fn checked_increment(value: usize) -> Result<usize, EvaluationError> {
    value.checked_add(1).ok_or_else(invalid_state)
}
