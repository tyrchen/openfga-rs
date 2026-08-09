//! Bounded CEL parsing, static checking, and IR construction.

use std::{collections::BTreeMap, fmt, mem::size_of, panic::catch_unwind};

use cel_parser::{
    ParseErrors, Parser,
    ast::{CallExpr, ComprehensionExpr, EntryExpr, Expr, IdedExpr, MapEntryExpr, operators},
    reference::Val,
};
use openfga_domain::{
    ConditionContext, ConditionName, ContextValue, Fingerprint, FingerprintBuilder, ParameterName,
};

use crate::{
    ConditionContextError,
    error::{CompileError, CompileErrorKind, EvaluationError},
    evaluator::evaluate,
    ir::{
        BinaryOperator, Comprehension, Function, Node, NodeId, NodeKind, Program, StaticType,
        UnaryOperator,
    },
    types::{
        CancellationCheck, CompiledMetadata, ConditionDefinition, ConditionLimits,
        ConditionOutcome, EvaluationBudget, EvaluationContexts, ParameterType, ParameterTypeKind,
        ParameterTypeRef,
    },
    value::RuntimeValue,
};

/// Stateless compiler for validated `OpenFGA` condition definitions.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct ConditionCompiler;

impl ConditionCompiler {
    /// Parses, bounds, statically checks, and fingerprints a condition.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`CompileError`] for invalid syntax, unsupported CEL,
    /// unknown identifiers, type mismatches, or structural limit violations.
    pub fn compile(
        &self,
        definition: &ConditionDefinition,
        limits: &ConditionLimits,
    ) -> Result<CompiledCondition, CompileError> {
        validate_definition(definition, limits)?;
        let parsed = catch_unwind(|| Parser::new().parse(definition.expression()))
            .map_err(|_| CompileError::new(CompileErrorKind::Syntax, 0))?;
        let expression =
            parsed.map_err(|errors| copy_parse_error(&errors, definition.expression()))?;
        validate_ast_shape(&expression, limits)?;
        let mut lowerer = Lowerer::new(definition);
        let root = lowerer.lower(&expression)?;
        if lowerer.node(root)?.static_type != StaticType::Bool {
            return Err(CompileError::non_boolean(static_type_name(
                &lowerer.node(root)?.static_type,
            )));
        }
        let metadata = CompiledMetadata {
            fingerprint: fingerprint_definition(definition),
        };
        let estimated_owned_bytes = size_of::<CompiledCondition>()
            .saturating_add(definition.expression().len().saturating_mul(4))
            .saturating_add(definition.parameters().len().saturating_mul(
                size_of::<(ParameterName, ParameterType)>() + 4 * size_of::<usize>(),
            ))
            .saturating_add(lowerer.nodes.capacity().saturating_mul(size_of::<Node>()))
            .saturating_add(lowerer.nodes.len().saturating_mul(128));
        Ok(CompiledCondition {
            name: definition.name().clone(),
            parameters: definition.parameters().clone(),
            program: Program {
                nodes: lowerer.nodes,
                root,
            },
            metadata,
            runtime_value_bytes: limits.runtime_value_bytes(),
            runtime_collection_items: limits.runtime_collection_items(),
            estimated_owned_bytes,
        })
    }
}

const fn static_type_name(value: &StaticType) -> &'static str {
    match value {
        StaticType::Dyn => "dyn",
        StaticType::Null => "null_type",
        StaticType::Bool => "bool",
        StaticType::Int => "int",
        StaticType::Uint => "uint",
        StaticType::Double => "double",
        StaticType::String => "string",
        StaticType::Bytes => "bytes",
        StaticType::Duration => "duration",
        StaticType::Timestamp => "timestamp",
        StaticType::IpAddress => "ipaddress",
        StaticType::List(_) => "list",
        StaticType::Map(_) => "map",
    }
}

/// Immutable, thread-safe compiled condition state.
#[derive(Clone)]
#[non_exhaustive]
pub struct CompiledCondition {
    name: ConditionName,
    parameters: BTreeMap<ParameterName, ParameterType>,
    pub(crate) program: Program,
    metadata: CompiledMetadata,
    pub(crate) runtime_value_bytes: usize,
    pub(crate) runtime_collection_items: usize,
    estimated_owned_bytes: usize,
}

impl CompiledCondition {
    /// Evaluates with tuple context overlaying request context by parameter name.
    ///
    /// # Errors
    ///
    /// Returns [`EvaluationError`] for missing or invalid parameters, runtime type
    /// failures, invalid helper inputs, arithmetic errors, exhausted cost, or cancellation.
    pub fn evaluate(
        &self,
        request_context: &ConditionContext,
        tuple_context: &ConditionContext,
        budget: EvaluationBudget,
        cancellation: &dyn CancellationCheck,
    ) -> Result<ConditionOutcome, EvaluationError> {
        evaluate(
            self,
            EvaluationContexts {
                request: request_context,
                tuple: tuple_context,
            },
            budget,
            cancellation,
        )
    }

    /// Returns the public condition name.
    #[must_use]
    pub const fn name(&self) -> &ConditionName {
        &self.name
    }

    /// Returns the deterministic compiled cache fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.metadata.fingerprint
    }

    /// Validates every supplied persisted tuple-context entry against declared parameters.
    ///
    /// Missing entries remain valid because request context may supply them during evaluation.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic for an unknown parameter or incompatible value.
    pub fn validate_context(
        &self,
        context: &ConditionContext,
    ) -> Result<(), ConditionContextError> {
        for (name, value) in context {
            let Some(parameter_type) = self.parameters.get(name) else {
                return Err(ConditionContextError::unknown(name.clone()));
            };
            crate::value::convert_parameter(value, parameter_type).map_err(|_| {
                ConditionContextError::invalid(
                    name.clone(),
                    parameter_type_name(parameter_type),
                    context_value_type(value),
                )
            })?;
        }
        Ok(())
    }

    /// Returns declared parameter types in canonical name order.
    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<ParameterName, ParameterType> {
        &self.parameters
    }

    /// Returns a conservative estimate of heap and inline bytes owned by this program.
    #[must_use]
    pub const fn estimated_owned_bytes(&self) -> usize {
        self.estimated_owned_bytes
    }
}

const fn parameter_type_name(parameter_type: &ParameterType) -> &'static str {
    match parameter_type.as_ref() {
        ParameterTypeRef::Any => "interface {}",
        ParameterTypeRef::Bool => "bool",
        ParameterTypeRef::String
        | ParameterTypeRef::Duration
        | ParameterTypeRef::Timestamp
        | ParameterTypeRef::IpAddress => "string",
        ParameterTypeRef::Int => "int64",
        ParameterTypeRef::Uint => "uint64",
        ParameterTypeRef::Double => "float64",
        ParameterTypeRef::Bytes => "[]uint8",
        ParameterTypeRef::List(_) => "[]interface {}",
        ParameterTypeRef::Map(_) => "map[string]interface {}",
    }
}

const fn context_value_type(value: &ContextValue) -> &'static str {
    match value {
        ContextValue::Null => "<nil>",
        ContextValue::Bool(_) => "bool",
        ContextValue::Int(_) => "int64",
        ContextValue::Uint(_) => "uint64",
        ContextValue::Double(_) => "float64",
        ContextValue::String(_) => "string",
        ContextValue::Bytes(_) => "[]uint8",
        ContextValue::List(_) => "[]interface {}",
        ContextValue::Map(_) => "map[string]interface {}",
        _ => "unknown",
    }
}

impl fmt::Debug for CompiledCondition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledCondition")
            .field("name", &self.name)
            .field("parameters", &self.parameters.len())
            .field("nodes", &self.program.nodes.len())
            .field("fingerprint", &self.metadata.fingerprint)
            .finish_non_exhaustive()
    }
}

fn validate_definition(
    definition: &ConditionDefinition,
    limits: &ConditionLimits,
) -> Result<(), CompileError> {
    if definition.expression().is_empty()
        || definition.expression().len() > limits.expression_bytes()
    {
        return Err(CompileError::new(CompileErrorKind::LimitExceeded, 0));
    }
    if definition.parameters().len() > limits.parameters() {
        return Err(CompileError::new(CompileErrorKind::LimitExceeded, 0));
    }
    Ok(())
}

fn copy_parse_error(errors: &ParseErrors, expression: &str) -> CompileError {
    let offset = errors
        .errors
        .first()
        .and_then(|error| source_offset(expression, error.pos))
        .unwrap_or(0);
    CompileError::new(CompileErrorKind::Syntax, offset)
}

fn source_offset(expression: &str, position: (isize, isize)) -> Option<usize> {
    let line = usize::try_from(position.0).ok()?.checked_sub(1)?;
    let column = usize::try_from(position.1).ok()?.checked_sub(1)?;
    let preceding = expression
        .split_inclusive('\n')
        .take(line)
        .try_fold(0_usize, |total, segment| total.checked_add(segment.len()))?;
    preceding
        .checked_add(column)
        .map(|offset| offset.min(expression.len()))
}

#[derive(Default)]
struct AstStats {
    nodes: usize,
    identifiers: usize,
    literals: usize,
    comprehensions: usize,
}

fn validate_ast_shape(root: &IdedExpr, limits: &ConditionLimits) -> Result<(), CompileError> {
    let mut stack = vec![(root, 1_usize)];
    let mut stats = AstStats::default();
    while let Some((expression, depth)) = stack.pop() {
        stats.nodes = checked_count(stats.nodes, limits.ast_nodes())?;
        if depth > limits.ast_depth() {
            return Err(CompileError::new(CompileErrorKind::LimitExceeded, 0));
        }
        let next_depth = depth.checked_add(1).ok_or_else(limit_error)?;
        push_children(expression, next_depth, &mut stack, &mut stats, limits)?;
    }
    Ok(())
}

fn push_children<'a>(
    expression: &'a IdedExpr,
    depth: usize,
    stack: &mut Vec<(&'a IdedExpr, usize)>,
    stats: &mut AstStats,
    limits: &ConditionLimits,
) -> Result<(), CompileError> {
    match &expression.expr {
        Expr::Unspecified | Expr::Struct(_) => return Err(unsupported()),
        Expr::Ident(_) => {
            stats.identifiers = checked_count(stats.identifiers, limits.identifiers())?;
        }
        Expr::Literal(_) => stats.literals = checked_count(stats.literals, limits.literals())?,
        Expr::List(list) => {
            bound_items(list.elements.len(), limits)?;
            stack.extend(list.elements.iter().map(|item| (item, depth)));
        }
        Expr::Map(map) => {
            bound_items(map.entries.len(), limits)?;
            for entry in &map.entries {
                let EntryExpr::MapEntry(entry) = &entry.expr else {
                    return Err(unsupported());
                };
                if entry.optional {
                    return Err(unsupported());
                }
                stack.push((&entry.key, depth));
                stack.push((&entry.value, depth));
            }
        }
        Expr::Select(select) => stack.push((&select.operand, depth)),
        Expr::Call(call) => {
            if let Some(target) = &call.target {
                stack.push((target, depth));
            }
            stack.extend(call.args.iter().map(|argument| (argument, depth)));
        }
        Expr::Comprehension(comprehension) => {
            stats.comprehensions = checked_count(stats.comprehensions, limits.comprehensions())?;
            stack.extend([
                (&*comprehension.iter_range, depth),
                (&*comprehension.accu_init, depth),
                (&*comprehension.loop_cond, depth),
                (&*comprehension.loop_step, depth),
                (&*comprehension.result, depth),
            ]);
        }
    }
    Ok(())
}

fn bound_items(items: usize, limits: &ConditionLimits) -> Result<(), CompileError> {
    if items > limits.literal_collection_items() {
        Err(limit_error())
    } else {
        Ok(())
    }
}

fn checked_count(current: usize, maximum: usize) -> Result<usize, CompileError> {
    let next = current.checked_add(1).ok_or_else(limit_error)?;
    if next > maximum {
        Err(limit_error())
    } else {
        Ok(next)
    }
}

const fn limit_error() -> CompileError {
    CompileError::new(CompileErrorKind::LimitExceeded, 0)
}
const fn unsupported() -> CompileError {
    CompileError::new(CompileErrorKind::Unsupported, 0)
}
const fn mismatch() -> CompileError {
    CompileError::new(CompileErrorKind::TypeMismatch, 0)
}

struct Lowerer<'a> {
    definition: &'a ConditionDefinition,
    nodes: Vec<Node>,
    scopes: Vec<BTreeMap<String, StaticType>>,
}

impl<'a> Lowerer<'a> {
    fn new(definition: &'a ConditionDefinition) -> Self {
        Self {
            definition,
            nodes: Vec::new(),
            scopes: Vec::new(),
        }
    }

    fn lower(&mut self, expression: &IdedExpr) -> Result<NodeId, CompileError> {
        match &expression.expr {
            Expr::Literal(value) => self.lower_literal(value),
            Expr::Ident(name) => self.lower_identifier(name),
            Expr::List(list) => self.lower_list(&list.elements),
            Expr::Map(map) => self.lower_map(&map.entries),
            Expr::Select(select) => self.lower_select(&select.operand, &select.field, select.test),
            Expr::Call(call) => self.lower_call(call),
            Expr::Comprehension(comprehension) => self.lower_comprehension(comprehension),
            Expr::Unspecified | Expr::Struct(_) => Err(unsupported()),
        }
    }

    fn push(&mut self, kind: NodeKind, static_type: StaticType) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node { kind, static_type });
        id
    }

    fn node(&self, id: NodeId) -> Result<&Node, CompileError> {
        self.nodes.get(id).ok_or_else(unsupported)
    }

    fn lower_literal(&mut self, value: &Val) -> Result<NodeId, CompileError> {
        let (value, static_type) = match value {
            Val::Null => (RuntimeValue::Null, StaticType::Null),
            Val::Boolean(value) => (RuntimeValue::Bool(*value), StaticType::Bool),
            Val::Int(value) => (RuntimeValue::Int(*value), StaticType::Int),
            Val::UInt(value) => (RuntimeValue::Uint(*value), StaticType::Uint),
            Val::Double(value) if value.is_finite() => {
                (RuntimeValue::Double(*value), StaticType::Double)
            }
            Val::Double(_) => return Err(mismatch()),
            Val::String(value) => (RuntimeValue::String(value.clone()), StaticType::String),
            Val::Bytes(value) => (RuntimeValue::Bytes(value.clone()), StaticType::Bytes),
        };
        Ok(self.push(NodeKind::Literal(value), static_type))
    }

    fn lower_identifier(&mut self, name: &str) -> Result<NodeId, CompileError> {
        for scope in self.scopes.iter().rev() {
            if let Some(static_type) = scope.get(name) {
                return Ok(self.push(NodeKind::Local(name.to_owned()), static_type.clone()));
            }
        }
        let Some((parameter, parameter_type)) = self
            .definition
            .parameters()
            .iter()
            .find(|(parameter, _)| parameter.as_str() == name)
        else {
            return Err(CompileError::unknown_identifier(name));
        };
        Ok(self.push(
            NodeKind::Parameter(parameter.clone()),
            static_type(parameter_type),
        ))
    }

    fn lower_list(&mut self, elements: &[IdedExpr]) -> Result<NodeId, CompileError> {
        let ids = elements
            .iter()
            .map(|element| self.lower(element))
            .collect::<Result<Vec<_>, _>>()?;
        let element_type = unify_nodes(&self.nodes, &ids)?;
        Ok(self.push(
            NodeKind::List(ids),
            StaticType::List(Box::new(element_type)),
        ))
    }

    fn lower_map(
        &mut self,
        entries: &[cel_parser::ast::IdedEntryExpr],
    ) -> Result<NodeId, CompileError> {
        let mut ids = Vec::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for entry in entries {
            let EntryExpr::MapEntry(MapEntryExpr {
                key,
                value,
                optional: false,
            }) = &entry.expr
            else {
                return Err(unsupported());
            };
            let key = self.lower(key)?;
            if self.node(key)?.static_type != StaticType::String {
                return Err(mismatch());
            }
            let value = self.lower(value)?;
            values.push(value);
            ids.push((key, value));
        }
        let value_type = unify_nodes(&self.nodes, &values)?;
        Ok(self.push(NodeKind::Map(ids), StaticType::Map(Box::new(value_type))))
    }

    fn lower_select(
        &mut self,
        operand: &IdedExpr,
        field: &str,
        test: bool,
    ) -> Result<NodeId, CompileError> {
        let operand = self.lower(operand)?;
        let value_type = match &self.node(operand)?.static_type {
            StaticType::Map(value) => (**value).clone(),
            StaticType::Dyn => StaticType::Dyn,
            _ => return Err(mismatch()),
        };
        let output = if test { StaticType::Bool } else { value_type };
        Ok(self.push(
            NodeKind::Select {
                operand,
                field: field.to_owned(),
                test,
            },
            output,
        ))
    }

    fn lower_call(&mut self, call: &CallExpr) -> Result<NodeId, CompileError> {
        match call.func_name.as_str() {
            operators::LOGICAL_AND => self.lower_logical(call, true),
            operators::LOGICAL_OR => self.lower_logical(call, false),
            operators::CONDITIONAL => self.lower_conditional(call),
            operators::LOGICAL_NOT => self.lower_unary(call, UnaryOperator::Not),
            operators::NEGATE => self.lower_unary(call, UnaryOperator::Negate),
            operators::NOT_STRICTLY_FALSE => {
                self.lower_unary(call, UnaryOperator::NotStrictlyFalse)
            }
            operators::ADD => self.lower_binary(call, BinaryOperator::Add),
            operators::SUBSTRACT => self.lower_binary(call, BinaryOperator::Subtract),
            operators::MULTIPLY => self.lower_binary(call, BinaryOperator::Multiply),
            operators::DIVIDE => self.lower_binary(call, BinaryOperator::Divide),
            operators::MODULO => self.lower_binary(call, BinaryOperator::Modulo),
            operators::EQUALS => self.lower_binary(call, BinaryOperator::Equal),
            operators::NOT_EQUALS => self.lower_binary(call, BinaryOperator::NotEqual),
            operators::GREATER => self.lower_binary(call, BinaryOperator::Greater),
            operators::GREATER_EQUALS => self.lower_binary(call, BinaryOperator::GreaterEqual),
            operators::LESS => self.lower_binary(call, BinaryOperator::Less),
            operators::LESS_EQUALS => self.lower_binary(call, BinaryOperator::LessEqual),
            operators::IN => self.lower_binary(call, BinaryOperator::In),
            operators::INDEX => self.lower_binary(call, BinaryOperator::Index),
            name => self.lower_function(call, name),
        }
    }

    fn lower_unary(
        &mut self,
        call: &CallExpr,
        operator: UnaryOperator,
    ) -> Result<NodeId, CompileError> {
        let [operand] = call.args.as_slice() else {
            return Err(unsupported());
        };
        if call.target.is_some() {
            return Err(unsupported());
        }
        let operand = self.lower(operand)?;
        let input = &self.node(operand)?.static_type;
        let output = match operator {
            UnaryOperator::Not | UnaryOperator::NotStrictlyFalse
                if StaticType::Bool.accepts(input) =>
            {
                StaticType::Bool
            }
            UnaryOperator::Negate
                if matches!(
                    input,
                    StaticType::Int | StaticType::Double | StaticType::Dyn
                ) =>
            {
                input.clone()
            }
            _ => return Err(mismatch()),
        };
        Ok(self.push(NodeKind::Unary { operator, operand }, output))
    }

    fn lower_logical(&mut self, call: &CallExpr, and: bool) -> Result<NodeId, CompileError> {
        let [left, right] = call.args.as_slice() else {
            return Err(unsupported());
        };
        let left = self.lower(left)?;
        let right = self.lower(right)?;
        if !StaticType::Bool.accepts(&self.node(left)?.static_type)
            || !StaticType::Bool.accepts(&self.node(right)?.static_type)
        {
            return Err(mismatch());
        }
        let kind = if and {
            NodeKind::LogicalAnd { left, right }
        } else {
            NodeKind::LogicalOr { left, right }
        };
        Ok(self.push(kind, StaticType::Bool))
    }

    fn lower_conditional(&mut self, call: &CallExpr) -> Result<NodeId, CompileError> {
        let [condition, truthy, falsy] = call.args.as_slice() else {
            return Err(unsupported());
        };
        let condition = self.lower(condition)?;
        let truthy = self.lower(truthy)?;
        let falsy = self.lower(falsy)?;
        if !StaticType::Bool.accepts(&self.node(condition)?.static_type) {
            return Err(mismatch());
        }
        let output = unify(
            &self.node(truthy)?.static_type,
            &self.node(falsy)?.static_type,
        )?;
        Ok(self.push(
            NodeKind::Conditional {
                condition,
                truthy,
                falsy,
            },
            output,
        ))
    }

    fn lower_binary(
        &mut self,
        call: &CallExpr,
        operator: BinaryOperator,
    ) -> Result<NodeId, CompileError> {
        let [left, right] = call.args.as_slice() else {
            return Err(unsupported());
        };
        if call.target.is_some() {
            return Err(unsupported());
        }
        let left = self.lower(left)?;
        let right = self.lower(right)?;
        let left_type = &self.node(left)?.static_type;
        let right_type = &self.node(right)?.static_type;
        let output = binary_type(operator, left_type, right_type).map_err(|error| {
            if error.kind() == CompileErrorKind::TypeMismatch {
                CompileError::no_matching_overload(
                    binary_function_name(operator),
                    vec![static_type_name(left_type), static_type_name(right_type)],
                )
            } else {
                error
            }
        })?;
        Ok(self.push(
            NodeKind::Binary {
                operator,
                left,
                right,
            },
            output,
        ))
    }

    fn lower_function(&mut self, call: &CallExpr, name: &str) -> Result<NodeId, CompileError> {
        let function = function(name, call.target.is_some()).ok_or_else(unsupported)?;
        let target = call
            .target
            .as_deref()
            .map(|target| self.lower(target))
            .transpose()?;
        let arguments = call
            .args
            .iter()
            .map(|argument| self.lower(argument))
            .collect::<Result<Vec<_>, _>>()?;
        let output = function_type(function, target, &arguments, &self.nodes).map_err(|error| {
            if matches!(
                error.kind(),
                CompileErrorKind::TypeMismatch | CompileErrorKind::Unsupported
            ) {
                let argument_types = target
                    .into_iter()
                    .chain(arguments.iter().copied())
                    .filter_map(|id| self.nodes.get(id))
                    .map(|node| static_type_name(&node.static_type))
                    .collect();
                CompileError::no_matching_overload(name, argument_types)
            } else {
                error
            }
        })?;
        Ok(self.push(
            NodeKind::Call {
                function,
                target,
                arguments,
            },
            output,
        ))
    }

    fn lower_comprehension(&mut self, source: &ComprehensionExpr) -> Result<NodeId, CompileError> {
        let iter_range = self.lower(&source.iter_range)?;
        let (iter_type, iter_type2) = iterable_types(&self.node(iter_range)?.static_type)?;
        let accu_init = self.lower(&source.accu_init)?;
        let accu_type = self.node(accu_init)?.static_type.clone();
        let mut scope = BTreeMap::new();
        scope.insert(source.iter_var.clone(), iter_type);
        if let (Some(name), Some(static_type)) = (&source.iter_var2, iter_type2) {
            scope.insert(name.clone(), static_type);
        }
        scope.insert(source.accu_var.clone(), accu_type.clone());
        self.scopes.push(scope);
        let lowered = self.lower_comprehension_scope(source, iter_range, accu_init, &accu_type);
        self.scopes.pop();
        lowered
    }

    fn lower_comprehension_scope(
        &mut self,
        source: &ComprehensionExpr,
        iter_range: NodeId,
        accu_init: NodeId,
        accu_type: &StaticType,
    ) -> Result<NodeId, CompileError> {
        let loop_cond = self.lower(&source.loop_cond)?;
        let loop_step = self.lower(&source.loop_step)?;
        let result = self.lower(&source.result)?;
        if !StaticType::Bool.accepts(&self.node(loop_cond)?.static_type)
            || !accu_type.accepts(&self.node(loop_step)?.static_type)
        {
            return Err(mismatch());
        }
        let output = self.node(result)?.static_type.clone();
        Ok(self.push(
            NodeKind::Comprehension(Comprehension {
                iter_range,
                iter_var: source.iter_var.clone(),
                iter_var2: source.iter_var2.clone(),
                accu_var: source.accu_var.clone(),
                accu_init,
                loop_cond,
                loop_step,
                result,
            }),
            output,
        ))
    }
}

const fn binary_function_name(operator: BinaryOperator) -> &'static str {
    match operator {
        BinaryOperator::Add => "_+_",
        BinaryOperator::Subtract => "_-_",
        BinaryOperator::Multiply => "_*_",
        BinaryOperator::Divide => "_/_",
        BinaryOperator::Modulo => "_%_",
        BinaryOperator::Equal => "_==_",
        BinaryOperator::NotEqual => "_!=_",
        BinaryOperator::Greater => "_>_",
        BinaryOperator::GreaterEqual => "_>=_",
        BinaryOperator::Less => "_<_",
        BinaryOperator::LessEqual => "_<=_",
        BinaryOperator::In => "_in_",
        BinaryOperator::Index => "_[_]",
    }
}

fn static_type(parameter: &ParameterType) -> StaticType {
    match &parameter.kind {
        ParameterTypeKind::Any => StaticType::Dyn,
        ParameterTypeKind::Bool => StaticType::Bool,
        ParameterTypeKind::String => StaticType::String,
        ParameterTypeKind::Int => StaticType::Int,
        ParameterTypeKind::Uint => StaticType::Uint,
        ParameterTypeKind::Double => StaticType::Double,
        ParameterTypeKind::Bytes => StaticType::Bytes,
        ParameterTypeKind::Duration => StaticType::Duration,
        ParameterTypeKind::Timestamp => StaticType::Timestamp,
        ParameterTypeKind::IpAddress => StaticType::IpAddress,
        ParameterTypeKind::List(value) => StaticType::List(Box::new(static_type(value))),
        ParameterTypeKind::Map(value) => StaticType::Map(Box::new(static_type(value))),
    }
}

fn unify_nodes(nodes: &[Node], ids: &[NodeId]) -> Result<StaticType, CompileError> {
    let mut current = StaticType::Dyn;
    for id in ids {
        let node = nodes.get(*id).ok_or_else(unsupported)?;
        current = if current == StaticType::Dyn {
            node.static_type.clone()
        } else {
            unify(&current, &node.static_type)?
        };
    }
    Ok(current)
}

fn unify(left: &StaticType, right: &StaticType) -> Result<StaticType, CompileError> {
    match (left, right) {
        (StaticType::Dyn, _) => Ok(right.clone()),
        (_, StaticType::Dyn) => Ok(left.clone()),
        (StaticType::List(left), StaticType::List(right)) => {
            unify(left, right).map(|value| StaticType::List(Box::new(value)))
        }
        (StaticType::Map(left), StaticType::Map(right)) => {
            unify(left, right).map(|value| StaticType::Map(Box::new(value)))
        }
        _ if left == right => Ok(left.clone()),
        _ => Err(mismatch()),
    }
}

fn binary_type(
    operator: BinaryOperator,
    left: &StaticType,
    right: &StaticType,
) -> Result<StaticType, CompileError> {
    use BinaryOperator::{
        Add, Divide, Equal, Greater, GreaterEqual, In, Index, Less, LessEqual, Modulo, Multiply,
        NotEqual, Subtract,
    };
    match operator {
        Equal | NotEqual if left.accepts(right) => Ok(StaticType::Bool),
        Greater | GreaterEqual | Less | LessEqual if comparable(left) && left.accepts(right) => {
            Ok(StaticType::Bool)
        }
        Add if left.accepts(right)
            && matches!(
                left,
                StaticType::Int
                    | StaticType::Uint
                    | StaticType::Double
                    | StaticType::String
                    | StaticType::Bytes
                    | StaticType::List(_)
                    | StaticType::Dyn
            ) =>
        {
            Ok(left.clone())
        }
        Subtract | Multiply | Divide | Modulo
            if left.accepts(right)
                && matches!(
                    left,
                    StaticType::Int | StaticType::Uint | StaticType::Double | StaticType::Dyn
                ) =>
        {
            Ok(left.clone())
        }
        In => membership_type(left, right),
        Index => index_type(left, right),
        _ => Err(mismatch()),
    }
}

fn comparable(value: &StaticType) -> bool {
    matches!(
        value,
        StaticType::Int
            | StaticType::Uint
            | StaticType::Double
            | StaticType::String
            | StaticType::Bytes
            | StaticType::Duration
            | StaticType::Timestamp
            | StaticType::Dyn
    )
}

fn membership_type(left: &StaticType, right: &StaticType) -> Result<StaticType, CompileError> {
    match right {
        StaticType::List(element) if element.accepts(left) => Ok(StaticType::Bool),
        StaticType::Map(_) if StaticType::String.accepts(left) => Ok(StaticType::Bool),
        StaticType::Dyn => Ok(StaticType::Bool),
        _ => Err(mismatch()),
    }
}

fn index_type(left: &StaticType, right: &StaticType) -> Result<StaticType, CompileError> {
    match left {
        StaticType::List(element)
            if matches!(right, StaticType::Int | StaticType::Uint | StaticType::Dyn) =>
        {
            Ok((**element).clone())
        }
        StaticType::Map(value) if StaticType::String.accepts(right) => Ok((**value).clone()),
        StaticType::Dyn => Ok(StaticType::Dyn),
        _ => Err(mismatch()),
    }
}

fn function(name: &str, receiver: bool) -> Option<Function> {
    match (receiver, name) {
        (false, "duration") => Some(Function::Duration),
        (false, "timestamp") => Some(Function::Timestamp),
        (false, "ipaddress") => Some(Function::IpAddress),
        (false, "int") => Some(Function::Int),
        (false, "uint") => Some(Function::Uint),
        (false, "double") => Some(Function::Double),
        (false, "string") => Some(Function::String),
        (false, "bytes") => Some(Function::Bytes),
        (false | true, "size") => Some(Function::Size),
        (true, "contains") => Some(Function::Contains),
        (true, "startsWith") => Some(Function::StartsWith),
        (true, "endsWith") => Some(Function::EndsWith),
        (true, "in_cidr") => Some(Function::InCidr),
        _ => None,
    }
}

fn function_type(
    function: Function,
    target: Option<NodeId>,
    args: &[NodeId],
    nodes: &[Node],
) -> Result<StaticType, CompileError> {
    let argument_types = args
        .iter()
        .map(|id| node_type(nodes, *id))
        .collect::<Result<Vec<_>, _>>()?;
    let target_type = target.map(|id| node_type(nodes, id)).transpose()?;
    match (function, target_type, argument_types.as_slice()) {
        (Function::Duration, None, [StaticType::String]) => Ok(StaticType::Duration),
        (Function::Timestamp, None, [StaticType::String]) => Ok(StaticType::Timestamp),
        (Function::IpAddress, None, [StaticType::String]) => Ok(StaticType::IpAddress),
        (
            Function::Int,
            None,
            [
                StaticType::Int
                | StaticType::Uint
                | StaticType::Double
                | StaticType::String
                | StaticType::Dyn,
            ],
        )
        | (
            Function::Size,
            Some(
                StaticType::String
                | StaticType::Bytes
                | StaticType::List(_)
                | StaticType::Map(_)
                | StaticType::Dyn,
            ),
            [],
        )
        | (
            Function::Size,
            None,
            [
                StaticType::String
                | StaticType::Bytes
                | StaticType::List(_)
                | StaticType::Map(_)
                | StaticType::Dyn,
            ],
        ) => Ok(StaticType::Int),
        (
            Function::Uint,
            None,
            [
                StaticType::Int
                | StaticType::Uint
                | StaticType::Double
                | StaticType::String
                | StaticType::Dyn,
            ],
        ) => Ok(StaticType::Uint),
        (
            Function::Double,
            None,
            [
                StaticType::Int
                | StaticType::Uint
                | StaticType::Double
                | StaticType::String
                | StaticType::Dyn,
            ],
        ) => Ok(StaticType::Double),
        (
            Function::String,
            None,
            [
                StaticType::String
                | StaticType::Int
                | StaticType::Uint
                | StaticType::Double
                | StaticType::Bool
                | StaticType::Bytes
                | StaticType::Dyn,
            ],
        ) => Ok(StaticType::String),
        (Function::Bytes, None, [StaticType::String | StaticType::Dyn]) => Ok(StaticType::Bytes),
        (
            Function::Contains | Function::StartsWith | Function::EndsWith,
            Some(StaticType::String | StaticType::Dyn),
            [StaticType::String | StaticType::Dyn],
        )
        | (
            Function::InCidr,
            Some(StaticType::IpAddress | StaticType::Dyn),
            [StaticType::String | StaticType::Dyn],
        ) => Ok(StaticType::Bool),
        _ => Err(mismatch()),
    }
}

fn node_type(nodes: &[Node], id: NodeId) -> Result<&StaticType, CompileError> {
    nodes
        .get(id)
        .map(|node| &node.static_type)
        .ok_or_else(unsupported)
}

fn iterable_types(value: &StaticType) -> Result<(StaticType, Option<StaticType>), CompileError> {
    match value {
        StaticType::List(element) => Ok(((**element).clone(), None)),
        StaticType::Map(value) => Ok((StaticType::String, Some((**value).clone()))),
        StaticType::Dyn => Ok((StaticType::Dyn, Some(StaticType::Dyn))),
        _ => Err(mismatch()),
    }
}

pub(crate) fn fingerprint_definition(definition: &ConditionDefinition) -> Fingerprint {
    let mut builder = FingerprintBuilder::new("openfga.condition.compiled.v1");
    builder.write_str(definition.name().as_str());
    builder.write_str(definition.expression());
    builder.write_u64(u64::try_from(definition.parameters().len()).unwrap_or(u64::MAX));
    for (name, parameter_type) in definition.parameters() {
        builder.write_str(name.as_str());
        fingerprint_type(parameter_type, &mut builder);
    }
    builder.finish()
}

fn fingerprint_type(parameter_type: &ParameterType, builder: &mut FingerprintBuilder) {
    match &parameter_type.kind {
        ParameterTypeKind::Any => builder.write_tag(0),
        ParameterTypeKind::Bool => builder.write_tag(1),
        ParameterTypeKind::String => builder.write_tag(2),
        ParameterTypeKind::Int => builder.write_tag(3),
        ParameterTypeKind::Uint => builder.write_tag(4),
        ParameterTypeKind::Double => builder.write_tag(5),
        ParameterTypeKind::Bytes => builder.write_tag(6),
        ParameterTypeKind::Duration => builder.write_tag(7),
        ParameterTypeKind::Timestamp => builder.write_tag(8),
        ParameterTypeKind::IpAddress => builder.write_tag(9),
        ParameterTypeKind::List(value) => {
            builder.write_tag(10);
            fingerprint_type(value, builder);
        }
        ParameterTypeKind::Map(value) => {
            builder.write_tag(11);
            fingerprint_type(value, builder);
        }
    }
}
