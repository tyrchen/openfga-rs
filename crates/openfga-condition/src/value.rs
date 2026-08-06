//! Private runtime values and lossless boundary conversion.

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use jiff::{SignedDuration, Timestamp};
use openfga_domain::{ContextValue, ParameterName};

use crate::{
    error::{EvaluationError, EvaluationErrorKind},
    types::{ParameterType, ParameterTypeKind},
};

#[derive(Clone, Debug)]
pub(crate) enum RuntimeValue {
    Unknown(BTreeSet<ParameterName>),
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Duration(SignedDuration),
    Timestamp(Timestamp),
    IpAddress(IpAddr),
    List(Vec<Self>),
    Map(Vec<(String, Self)>),
}

pub(crate) fn convert_parameter(
    value: &ContextValue,
    parameter_type: &ParameterType,
) -> Result<RuntimeValue, EvaluationError> {
    convert_kind(value, &parameter_type.kind)
        .map_err(|_| EvaluationError::new(EvaluationErrorKind::InvalidParameter))
}

fn convert_kind(
    value: &ContextValue,
    kind: &ParameterTypeKind,
) -> Result<RuntimeValue, EvaluationError> {
    match kind {
        ParameterTypeKind::Any => convert_any(value),
        ParameterTypeKind::Bool => match value {
            ContextValue::Bool(value) => Ok(RuntimeValue::Bool(*value)),
            _ => invalid_parameter(),
        },
        ParameterTypeKind::String => match value {
            ContextValue::String(value) => Ok(RuntimeValue::String(value.as_str().to_owned())),
            _ => invalid_parameter(),
        },
        ParameterTypeKind::Int => convert_int(value).map(RuntimeValue::Int),
        ParameterTypeKind::Uint => convert_uint(value).map(RuntimeValue::Uint),
        ParameterTypeKind::Double => convert_double(value).map(RuntimeValue::Double),
        ParameterTypeKind::Bytes => match value {
            ContextValue::Bytes(value) => Ok(RuntimeValue::Bytes(value.as_slice().to_vec())),
            _ => invalid_parameter(),
        },
        ParameterTypeKind::Duration => match value {
            ContextValue::String(value) => {
                parse_duration(value.as_str()).map(RuntimeValue::Duration)
            }
            _ => invalid_parameter(),
        },
        ParameterTypeKind::Timestamp => match value {
            ContextValue::String(value) => {
                parse_timestamp(value.as_str()).map(RuntimeValue::Timestamp)
            }
            _ => invalid_parameter(),
        },
        ParameterTypeKind::IpAddress => match value {
            ContextValue::String(value) => {
                parse_ip_address(value.as_str()).map(RuntimeValue::IpAddress)
            }
            _ => invalid_parameter(),
        },
        ParameterTypeKind::List(element_type) => match value {
            ContextValue::List(values) => values
                .as_slice()
                .iter()
                .map(|value| convert_kind(value, &element_type.kind))
                .collect::<Result<Vec<_>, _>>()
                .map(RuntimeValue::List),
            _ => invalid_parameter(),
        },
        ParameterTypeKind::Map(value_type) => match value {
            ContextValue::Map(values) => {
                let mut converted = values
                    .iter()
                    .map(|(key, value)| {
                        convert_kind(value, &value_type.kind)
                            .map(|value| (key.as_str().to_owned(), value))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                converted.sort_by(|(left, _), (right, _)| left.cmp(right));
                Ok(RuntimeValue::Map(converted))
            }
            _ => invalid_parameter(),
        },
    }
}

fn convert_any(value: &ContextValue) -> Result<RuntimeValue, EvaluationError> {
    match value {
        ContextValue::Null => Ok(RuntimeValue::Null),
        ContextValue::Bool(value) => Ok(RuntimeValue::Bool(*value)),
        ContextValue::Int(value) => Ok(RuntimeValue::Int(*value)),
        ContextValue::Uint(value) => Ok(RuntimeValue::Uint(*value)),
        ContextValue::Double(value) => Ok(RuntimeValue::Double(value.get())),
        ContextValue::String(value) => Ok(RuntimeValue::String(value.as_str().to_owned())),
        ContextValue::Bytes(value) => Ok(RuntimeValue::Bytes(value.as_slice().to_vec())),
        ContextValue::List(values) => values
            .as_slice()
            .iter()
            .map(convert_any)
            .collect::<Result<Vec<_>, _>>()
            .map(RuntimeValue::List),
        ContextValue::Map(values) => {
            let mut converted = values
                .iter()
                .map(|(key, value)| {
                    convert_any(value).map(|value| (key.as_str().to_owned(), value))
                })
                .collect::<Result<Vec<_>, _>>()?;
            converted.sort_by(|(left, _), (right, _)| left.cmp(right));
            Ok(RuntimeValue::Map(converted))
        }
        _ => invalid_parameter(),
    }
}

fn convert_int(value: &ContextValue) -> Result<i64, EvaluationError> {
    match value {
        ContextValue::Int(value) => Ok(*value),
        ContextValue::Double(value) => exact_f64_to_i64(value.get()).ok_or_else(invalid_error),
        ContextValue::String(value) => {
            parse_i64_parameter(value.as_str()).ok_or_else(invalid_error)
        }
        _ => invalid_parameter(),
    }
}

fn convert_uint(value: &ContextValue) -> Result<u64, EvaluationError> {
    match value {
        ContextValue::Uint(value) => Ok(*value),
        ContextValue::Double(value) => exact_f64_to_u64(value.get()).ok_or_else(invalid_error),
        ContextValue::String(value) => {
            parse_u64_parameter(value.as_str()).ok_or_else(invalid_error)
        }
        _ => invalid_parameter(),
    }
}

fn convert_double(value: &ContextValue) -> Result<f64, EvaluationError> {
    let converted = match value {
        ContextValue::Double(value) => value.get(),
        ContextValue::String(value) => value.as_str().parse().map_err(|_| invalid_error())?,
        _ => return invalid_parameter(),
    };
    converted
        .is_finite()
        .then_some(converted)
        .ok_or_else(invalid_error)
}

fn exact_f64_to_i64(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    value.to_string().parse().ok()
}

fn exact_f64_to_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value.fract() != 0.0 || value.is_sign_negative() {
        return None;
    }
    value.to_string().parse().ok()
}

fn parse_i64_parameter(value: &str) -> Option<i64> {
    value
        .parse()
        .ok()
        .or_else(|| value.parse::<f64>().ok().and_then(exact_f64_to_i64))
}

fn parse_u64_parameter(value: &str) -> Option<u64> {
    value
        .parse()
        .ok()
        .or_else(|| value.parse::<f64>().ok().and_then(exact_f64_to_u64))
}

pub(crate) fn parse_duration(value: &str) -> Result<SignedDuration, EvaluationError> {
    let valid = !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_digit()
                || matches!(
                    character,
                    '+' | '-' | '.' | 'h' | 'm' | 's' | 'u' | 'n' | 'µ' | 'μ'
                )
        });
    if !valid {
        return Err(invalid_value_error());
    }
    value
        .replace('μ', "µ")
        .parse()
        .map_err(|_| invalid_value_error())
}

pub(crate) fn parse_timestamp(value: &str) -> Result<Timestamp, EvaluationError> {
    if !has_rfc3339_shape(value) {
        return Err(invalid_value_error());
    }
    value.parse().map_err(|_| invalid_value_error())
}

fn has_rfc3339_shape(value: &str) -> bool {
    let bytes = value.as_bytes();
    let fixed = bytes.get(0..4).is_some_and(all_ascii_digits)
        && bytes.get(4) == Some(&b'-')
        && bytes.get(5..7).is_some_and(all_ascii_digits)
        && bytes.get(7) == Some(&b'-')
        && bytes.get(8..10).is_some_and(all_ascii_digits)
        && bytes.get(10) == Some(&b'T')
        && bytes.get(11..13).is_some_and(all_ascii_digits)
        && bytes.get(13) == Some(&b':')
        && bytes.get(14..16).is_some_and(all_ascii_digits)
        && bytes.get(16) == Some(&b':')
        && bytes.get(17..19).is_some_and(all_ascii_digits)
        && bytes.get(17..19) != Some(b"60");
    fixed && bytes.get(19..).is_some_and(valid_rfc3339_suffix)
}

fn valid_rfc3339_suffix(suffix: &[u8]) -> bool {
    if suffix == b"Z" {
        return true;
    }
    let Some(timezone_start) = suffix
        .iter()
        .position(|byte| matches!(byte, b'Z' | b'+' | b'-'))
    else {
        return false;
    };
    let fraction = suffix.get(..timezone_start);
    let timezone = suffix.get(timezone_start..);
    let valid_fraction = fraction.is_some_and(|fraction| {
        fraction.is_empty()
            || (fraction.first() == Some(&b'.')
                && fraction
                    .get(1..)
                    .is_some_and(|digits| !digits.is_empty() && all_ascii_digits(digits)))
    });
    valid_fraction && timezone.is_some_and(valid_timezone)
}

fn valid_timezone(timezone: &[u8]) -> bool {
    timezone == b"Z"
        || (timezone.len() == 6
            && timezone
                .first()
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            && timezone.get(1..3).is_some_and(all_ascii_digits)
            && timezone.get(3) == Some(&b':')
            && timezone.get(4..6).is_some_and(all_ascii_digits))
}

fn all_ascii_digits(value: &[u8]) -> bool {
    value.iter().all(u8::is_ascii_digit)
}

pub(crate) fn parse_ip_address(value: &str) -> Result<IpAddr, EvaluationError> {
    IpAddr::from_str(value)
        .map(normalize_ip)
        .map_err(|_| invalid_value_error())
}

fn normalize_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map_or(IpAddr::V6(address), IpAddr::V4),
        address @ IpAddr::V4(_) => address,
    }
}

pub(crate) fn ip_in_cidr(address: IpAddr, cidr: &str) -> Result<bool, EvaluationError> {
    let (network, prefix) = cidr.split_once('/').ok_or_else(invalid_value_error)?;
    let network = parse_ip_address(network)?;
    let prefix = prefix.parse::<u8>().map_err(|_| invalid_value_error())?;
    match (normalize_ip(address), network) {
        (IpAddr::V4(address), IpAddr::V4(network)) => cidr_v4(address, network, prefix),
        (IpAddr::V6(address), IpAddr::V6(network)) => cidr_v6(address, network, prefix),
        _ => Err(invalid_value_error()),
    }
}

fn cidr_v4(address: Ipv4Addr, network: Ipv4Addr, prefix: u8) -> Result<bool, EvaluationError> {
    if prefix > 32 {
        return Err(invalid_value_error());
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    Ok(u32::from(address) & mask == u32::from(network) & mask)
}

fn cidr_v6(address: Ipv6Addr, network: Ipv6Addr, prefix: u8) -> Result<bool, EvaluationError> {
    if prefix > 128 {
        return Err(invalid_value_error());
    }
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    Ok(u128::from(address) & mask == u128::from(network) & mask)
}

pub(crate) fn compare(left: &RuntimeValue, right: &RuntimeValue) -> Option<Ordering> {
    match (left, right) {
        (RuntimeValue::Int(left), RuntimeValue::Int(right)) => left.partial_cmp(right),
        (RuntimeValue::Uint(left), RuntimeValue::Uint(right)) => left.partial_cmp(right),
        (RuntimeValue::Double(left), RuntimeValue::Double(right)) => left.partial_cmp(right),
        (RuntimeValue::String(left), RuntimeValue::String(right)) => left.partial_cmp(right),
        (RuntimeValue::Bytes(left), RuntimeValue::Bytes(right)) => left.partial_cmp(right),
        (RuntimeValue::Duration(left), RuntimeValue::Duration(right)) => left.partial_cmp(right),
        (RuntimeValue::Timestamp(left), RuntimeValue::Timestamp(right)) => left.partial_cmp(right),
        _ => None,
    }
}

pub(crate) fn parameter_value<'a>(
    name: &ParameterName,
    request: &'a openfga_domain::ConditionContext,
    tuple: &'a openfga_domain::ConditionContext,
) -> Option<&'a ContextValue> {
    tuple.get(name).or_else(|| request.get(name))
}

fn invalid_parameter<T>() -> Result<T, EvaluationError> {
    Err(invalid_error())
}

const fn invalid_error() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::InvalidParameter)
}

const fn invalid_value_error() -> EvaluationError {
    EvaluationError::new(EvaluationErrorKind::InvalidValue)
}
