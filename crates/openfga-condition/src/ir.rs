//! Immutable project-owned condition intermediate representation.

use openfga_domain::ParameterName;

use crate::value::RuntimeValue;

pub(crate) type NodeId = usize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StaticType {
    Dyn,
    Null,
    Bool,
    Int,
    Uint,
    Double,
    String,
    Bytes,
    Duration,
    Timestamp,
    IpAddress,
    List(Box<Self>),
    Map(Box<Self>),
}

impl StaticType {
    pub(crate) fn accepts(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Dyn, _) | (_, Self::Dyn) => true,
            (Self::List(left), Self::List(right)) | (Self::Map(left), Self::Map(right)) => {
                left.accepts(right)
            }
            _ => self == other,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOperator {
    Not,
    Negate,
    NotStrictlyFalse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    In,
    Index,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Function {
    Duration,
    Timestamp,
    IpAddress,
    Int,
    Uint,
    Double,
    String,
    Bytes,
    Size,
    Contains,
    StartsWith,
    EndsWith,
    InCidr,
}

#[derive(Clone, Debug)]
pub(crate) struct Comprehension {
    pub(crate) iter_range: NodeId,
    pub(crate) iter_var: String,
    pub(crate) iter_var2: Option<String>,
    pub(crate) accu_var: String,
    pub(crate) accu_init: NodeId,
    pub(crate) loop_cond: NodeId,
    pub(crate) loop_step: NodeId,
    pub(crate) result: NodeId,
}

#[derive(Clone, Debug)]
pub(crate) enum NodeKind {
    Literal(RuntimeValue),
    Parameter(ParameterName),
    Local(String),
    List(Vec<NodeId>),
    Map(Vec<(NodeId, NodeId)>),
    Select {
        operand: NodeId,
        field: String,
        test: bool,
    },
    Unary {
        operator: UnaryOperator,
        operand: NodeId,
    },
    Binary {
        operator: BinaryOperator,
        left: NodeId,
        right: NodeId,
    },
    LogicalAnd {
        left: NodeId,
        right: NodeId,
    },
    LogicalOr {
        left: NodeId,
        right: NodeId,
    },
    Conditional {
        condition: NodeId,
        truthy: NodeId,
        falsy: NodeId,
    },
    Call {
        function: Function,
        target: Option<NodeId>,
        arguments: Vec<NodeId>,
    },
    Comprehension(Comprehension),
}

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub(crate) kind: NodeKind,
    pub(crate) static_type: StaticType,
}

#[derive(Clone, Debug)]
pub(crate) struct Program {
    pub(crate) nodes: Vec<Node>,
    pub(crate) root: NodeId,
}
