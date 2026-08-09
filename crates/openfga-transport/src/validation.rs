//! Ordered exhaustive validation over reflected PGV rules.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    num::NonZeroU64,
    sync::{Arc, LazyLock},
    time::SystemTime,
};

use axum::body::Bytes;
use moka::sync::Cache;
use prost_reflect::{
    DynamicMessage, EnumDescriptor, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    ReflectMessage, Value,
};
use prost_validate::{
    Error as ValidationError,
    errors::{self as validation, r#enum, list, map, message, string, timestamp},
};
use prost_validate_types::{
    EnumRules, FieldRules, FieldRulesExt, Int32Rules, StringRules, TimestampRules,
    field_rules::Type as RuleType,
};
use regex::Regex;

const INVALID_TIMESTAMP_PREFIX: &str = "openfga_invalid_timestamp:";
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;
const ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WireCacheKey {
    message: Arc<str>,
    scope: Arc<str>,
    payload: Bytes,
}

impl WireCacheKey {
    fn new(message: &str, scope: &str, payload: Bytes) -> Self {
        Self {
            message: Arc::from(message),
            scope: Arc::from(scope),
            payload,
        }
    }

    fn estimated_weight(&self) -> usize {
        self.message
            .len()
            .saturating_add(self.scope.len())
            .saturating_add(self.payload.len())
            .saturating_add(ESTIMATED_CACHE_ENTRY_OVERHEAD_BYTES)
    }
}

/// Exact HTTP request identity carried from JSON normalization to typed PGV validation.
#[derive(Clone, Debug)]
pub(crate) struct HttpValidationKey(WireCacheKey);

tokio::task_local! {
    static HTTP_VALIDATION_KEY: HttpValidationKey;
}

pub(crate) async fn with_http_validation_key<F>(key: HttpValidationKey, future: F) -> F::Output
where
    F: Future,
{
    HTTP_VALIDATION_KEY.scope(key, future).await
}

/// Bounded exact-byte memoization for successful wire normalization and validation.
#[derive(Clone)]
pub(crate) struct WireCache {
    normalized_json: Cache<WireCacheKey, Bytes>,
    validated_messages: Cache<WireCacheKey, ()>,
}

impl WireCache {
    pub(crate) fn new(maximum_weight: NonZeroU64) -> Self {
        let validation_weight = maximum_weight.get() / 2;
        let normalization_weight = maximum_weight.get().saturating_sub(validation_weight);
        Self {
            normalized_json: Cache::builder()
                .max_capacity(normalization_weight)
                .weigher(|key: &WireCacheKey, normalized: &Bytes| {
                    cache_weight(key.estimated_weight().saturating_add(normalized.len()))
                })
                .build(),
            validated_messages: Cache::builder()
                .max_capacity(validation_weight)
                .weigher(|key: &WireCacheKey, (): &()| cache_weight(key.estimated_weight()))
                .build(),
        }
    }

    pub(crate) fn normalized_json(
        &self,
        descriptor: &MessageDescriptor,
        payload: &Bytes,
    ) -> Option<Bytes> {
        self.normalized_json.get(&WireCacheKey::new(
            descriptor.full_name(),
            "",
            payload.clone(),
        ))
    }

    pub(crate) fn cache_normalized_json(
        &self,
        descriptor: &MessageDescriptor,
        payload: Bytes,
        normalized: Bytes,
    ) {
        self.normalized_json.insert(
            WireCacheKey::new(descriptor.full_name(), "", payload),
            normalized,
        );
    }

    pub(crate) fn http_validation_key(
        descriptor: &MessageDescriptor,
        path: &str,
        payload: Bytes,
    ) -> HttpValidationKey {
        HttpValidationKey(WireCacheKey::new(descriptor.full_name(), path, payload))
    }

    fn validated(&self, key: &WireCacheKey) -> bool {
        self.validated_messages.get(key).is_some()
    }

    fn cache_validated(&self, key: WireCacheKey) {
        self.validated_messages.insert(key, ());
    }

    #[cfg(test)]
    fn validated_message_count(&self) -> u64 {
        self.validated_messages.run_pending_tasks();
        self.validated_messages.entry_count()
    }
}

impl fmt::Debug for WireCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WireCache")
            .field(
                "normalized_json_entries",
                &self.normalized_json.entry_count(),
            )
            .field(
                "validated_message_entries",
                &self.validated_messages.entry_count(),
            )
            .finish_non_exhaustive()
    }
}

fn cache_weight(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[derive(Debug)]
struct ValidationSchema {
    rules: HashMap<String, FieldRules>,
    regexes: HashMap<String, Regex>,
    time_dependent_messages: HashSet<String>,
}

static VALIDATION_SCHEMA: LazyLock<ValidationSchema> = LazyLock::new(|| {
    let mut rules = HashMap::new();
    let mut regexes = HashMap::new();
    let messages = openfga_proto::DESCRIPTOR_POOL
        .all_messages()
        .collect::<Vec<_>>();
    let mut time_dependent_messages = HashSet::new();
    for field in messages
        .iter()
        .flat_map(|message| message.fields().collect::<Vec<_>>())
    {
        let Ok(Some(field_rules)) = field.validation_rules() else {
            continue;
        };
        if let Some(RuleType::String(string_rules)) = &field_rules.r#type
            && let Some(pattern) = &string_rules.pattern
            && let Some(regex) = compile_go_regex(pattern)
        {
            regexes.insert(pattern.clone(), regex);
        }
        if matches!(
            &field_rules.r#type,
            Some(RuleType::Timestamp(timestamp_rules)) if timestamp_rules.lt_now()
        ) {
            time_dependent_messages.insert(field.parent_message().full_name().to_owned());
        }
        rules.insert(field.full_name().to_owned(), field_rules);
    }
    loop {
        let mut changed = false;
        for message in &messages {
            if time_dependent_messages.contains(message.full_name()) {
                continue;
            }
            let contains_time_dependent_message = message.fields().any(|field| {
                matches!(
                    field.kind(),
                    Kind::Message(nested)
                        if time_dependent_messages.contains(nested.full_name())
                )
            });
            if contains_time_dependent_message {
                changed |= time_dependent_messages.insert(message.full_name().to_owned());
            }
        }
        if !changed {
            break;
        }
    }
    ValidationSchema {
        rules,
        regexes,
        time_dependent_messages,
    }
});

/// Collects every PGV failure in the same field/rule/item order as generated `ValidateAll`.
#[cfg(test)]
pub(crate) fn validate_all<T: ReflectMessage>(request: &T) -> Vec<ValidationError> {
    validate_reflected(request)
}

/// Collects every PGV failure while memoizing only successful exact messages.
pub(crate) fn validate_all_cached<T: ReflectMessage>(
    cache: &WireCache,
    request: &T,
) -> Vec<ValidationError> {
    let descriptor = request.descriptor();
    if VALIDATION_SCHEMA
        .time_dependent_messages
        .contains(descriptor.full_name())
    {
        return validate_reflected(request);
    }
    let http_key = HTTP_VALIDATION_KEY
        .try_with(Clone::clone)
        .ok()
        .filter(|key| key.0.message.as_ref() == descriptor.full_name());
    let key = if let Some(key) = http_key {
        key.0
    } else {
        WireCacheKey::new(
            descriptor.full_name(),
            "",
            Bytes::from(request.encode_to_vec()),
        )
    };
    if cache.validated(&key) {
        return Vec::new();
    }
    let errors = validate_reflected(request);
    if errors.is_empty() {
        cache.cache_validated(key);
    }
    errors
}

fn validate_reflected<T: ReflectMessage>(request: &T) -> Vec<ValidationError> {
    let message = request.transcode_to_dynamic();
    let mut errors = Vec::new();
    validate_message(&message, &mut errors);
    errors
}

fn validate_message(candidate: &DynamicMessage, errors: &mut Vec<ValidationError>) {
    for field in candidate.descriptor().fields() {
        if let Some(rules) = VALIDATION_SCHEMA.rules.get(field.full_name()) {
            validate_field(candidate, &field, rules, errors);
        } else {
            validate_unruled_field(candidate, &field, errors);
        }
    }
}

fn validate_unruled_field(
    candidate: &DynamicMessage,
    field: &FieldDescriptor,
    errors: &mut Vec<ValidationError>,
) {
    let value = candidate.get_field(field);
    if field.is_list() {
        validate_list_messages(field, value.as_ref(), false, errors);
    } else if field.is_map() {
        validate_map_messages(field, value.as_ref(), false, errors);
    } else if candidate.has_field(field)
        && matches!(field.kind(), Kind::Message(_))
        && let Some(nested) = value.as_message()
    {
        let mut nested_errors = Vec::new();
        validate_message(nested, &mut nested_errors);
        errors.extend(nested_errors.into_iter().map(|error| {
            ValidationError::new(
                field.full_name(),
                validation::Error::Message(message::Error::Message(Box::new(error))),
            )
        }));
    }
}

fn validate_field(
    candidate: &DynamicMessage,
    field: &FieldDescriptor,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    let value = candidate.get_field(field);
    if field.is_list() {
        validate_list(field, value.as_ref(), rules, errors);
    } else if field.is_map() {
        validate_map(field, value.as_ref(), rules, errors);
    } else {
        validate_value(
            field,
            value.as_ref(),
            candidate.has_field(field),
            rules,
            errors,
        );
    }
}

fn validate_value(
    field: &FieldDescriptor,
    value: &Value,
    present: bool,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    match (&field.kind(), &rules.r#type) {
        (Kind::String, Some(RuleType::String(string_rules))) => {
            validate_string(field.full_name(), value, string_rules, errors);
        }
        (Kind::Enum(descriptor), Some(RuleType::Enum(enum_rules))) => {
            validate_enum(field.full_name(), value, descriptor, enum_rules, errors);
        }
        (Kind::Int32, Some(RuleType::Int32(number_rules))) => {
            validate_i32(field.full_name(), value, number_rules, errors);
        }
        (Kind::Message(descriptor), _) => {
            validate_message_value(field, value, present, descriptor, rules, errors);
        }
        _ => {}
    }
}

fn validate_message_value(
    field: &FieldDescriptor,
    value: &Value,
    present: bool,
    descriptor: &MessageDescriptor,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    if rules.message.is_some_and(|rules| rules.required()) && !present {
        errors.push(ValidationError::new(
            field.full_name(),
            validation::Error::Message(message::Error::Required),
        ));
        return;
    }
    if !present || rules.message.is_some_and(|rules| rules.skip()) {
        return;
    }
    let Some(nested) = value.as_message() else {
        return;
    };
    match descriptor.full_name() {
        "google.protobuf.Int32Value" => {
            if let (Some(RuleType::Int32(number_rules)), Some(value_field)) =
                (&rules.r#type, descriptor.get_field_by_name("value"))
            {
                validate_i32(
                    field.full_name(),
                    nested.get_field(&value_field).as_ref(),
                    number_rules,
                    errors,
                );
            }
        }
        "google.protobuf.Timestamp" => {
            if let Some(RuleType::Timestamp(timestamp_rules)) = &rules.r#type {
                validate_timestamp(field.full_name(), nested, timestamp_rules, errors);
            }
        }
        _ => {
            let mut nested_errors = Vec::new();
            validate_message(nested, &mut nested_errors);
            errors.extend(nested_errors.into_iter().map(|error| {
                ValidationError::new(
                    field.full_name(),
                    validation::Error::Message(message::Error::Message(Box::new(error))),
                )
            }));
        }
    }
}

fn validate_string(
    field: &str,
    value: &Value,
    rules: &StringRules,
    errors: &mut Vec<ValidationError>,
) {
    let value = value.as_str().unwrap_or_default();
    if rules.ignore_empty() && value.is_empty() {
        return;
    }
    if let Some(expected) = &rules.r#const
        && value != expected
    {
        push_string(errors, field, string::Error::Const(expected.clone()));
    }
    let char_count = value.chars().count();
    if let Some(length) = rules.len.map(to_usize)
        && char_count != length
    {
        push_string(errors, field, string::Error::Len(length));
    }
    if let Some(minimum) = rules.min_len.map(to_usize)
        && char_count < minimum
    {
        push_string(errors, field, string::Error::MinLen(minimum));
    }
    if let Some(maximum) = rules.max_len.map(to_usize)
        && char_count > maximum
    {
        push_string(errors, field, string::Error::MaxLen(maximum));
    }
    if let Some(length) = rules.len_bytes.map(to_usize)
        && value.len() != length
    {
        push_string(errors, field, string::Error::LenBytes(length));
    }
    match (rules.min_bytes.map(to_usize), rules.max_bytes.map(to_usize)) {
        (Some(minimum), Some(maximum)) if value.len() < minimum || value.len() > maximum => {
            // PGV generates a single inclusive-range check when both bounds are present.
            push_string(errors, field, string::Error::MinLenBytes(minimum));
        }
        (Some(minimum), None) if value.len() < minimum => {
            push_string(errors, field, string::Error::MinLenBytes(minimum));
        }
        (None, Some(maximum)) if value.len() > maximum => {
            push_string(errors, field, string::Error::MaxLenBytes(maximum));
        }
        _ => {}
    }
    if let Some(pattern) = &rules.pattern
        && validation_regex(pattern).is_some_and(|regex| !regex.is_match(value))
    {
        push_string(errors, field, string::Error::Pattern(pattern.clone()));
    }
    if let Some(prefix) = &rules.prefix
        && !value.starts_with(prefix)
    {
        push_string(errors, field, string::Error::Prefix(prefix.clone()));
    }
    if let Some(suffix) = &rules.suffix
        && !value.ends_with(suffix)
    {
        push_string(errors, field, string::Error::Suffix(suffix.clone()));
    }
    if let Some(contains) = &rules.contains
        && !value.contains(contains)
    {
        push_string(errors, field, string::Error::Contains(contains.clone()));
    }
    if let Some(not_contains) = &rules.not_contains
        && value.contains(not_contains)
    {
        push_string(
            errors,
            field,
            string::Error::NotContains(not_contains.clone()),
        );
    }
    if !rules.r#in.is_empty() && !rules.r#in.iter().any(|allowed| allowed == value) {
        push_string(errors, field, string::Error::In(rules.r#in.clone()));
    }
    if rules.not_in.iter().any(|excluded| excluded == value) {
        push_string(errors, field, string::Error::NotIn(rules.not_in.clone()));
    }
}

fn validation_regex(pattern: &str) -> Option<&'static Regex> {
    VALIDATION_SCHEMA.regexes.get(pattern)
}

fn compile_go_regex(pattern: &str) -> Option<Regex> {
    let ascii_classes = pattern
        .replace(r"\s", r"\x09\x0A\x0C\x0D\x20")
        .replace(r"\w", "A-Za-z0-9_")
        .replace(r"\d", "0-9");
    Regex::new(&ascii_classes).ok()
}

fn push_string(errors: &mut Vec<ValidationError>, field: &str, error: string::Error) {
    errors.push(ValidationError::new(
        field,
        validation::Error::String(error),
    ));
}

fn validate_enum(
    field: &str,
    value: &Value,
    descriptor: &EnumDescriptor,
    rules: &EnumRules,
    errors: &mut Vec<ValidationError>,
) {
    let value = value.as_enum_number().unwrap_or_default();
    if let Some(expected) = rules.r#const
        && value != expected
    {
        errors.push(ValidationError::new(
            field,
            validation::Error::Enum(r#enum::Error::Const(expected)),
        ));
    }
    if !rules.r#in.is_empty() && !rules.r#in.contains(&value) {
        errors.push(ValidationError::new(
            field,
            validation::Error::Enum(r#enum::Error::In(rules.r#in.clone())),
        ));
    }
    if rules.not_in.contains(&value) {
        errors.push(ValidationError::new(
            field,
            validation::Error::Enum(r#enum::Error::NotIn(rules.not_in.clone())),
        ));
    }
    if rules.defined_only() && descriptor.get_value(value).is_none() {
        errors.push(ValidationError::new(
            field,
            validation::Error::Enum(r#enum::Error::DefinedOnly),
        ));
    }
}

fn validate_i32(field: &str, value: &Value, rules: &Int32Rules, errors: &mut Vec<ValidationError>) {
    let value = value.as_i32().unwrap_or_default();
    if rules.ignore_empty() && value == 0 {
        return;
    }
    if let Some(expected) = rules.r#const
        && value != expected
    {
        push_i32(errors, field, validation::int32::Error::Const(expected));
    }
    match (rules.lt, rules.lte, rules.gt, rules.gte) {
        (Some(lt), _, Some(gt), _) if lt > gt && (value <= gt || value >= lt) => {
            push_i32(
                errors,
                field,
                validation::int32::Error::in_range(false, gt, lt, false),
            );
        }
        (Some(lt), _, Some(gt), _) if lt <= gt && value >= lt && value <= gt => {
            push_i32(
                errors,
                field,
                validation::int32::Error::not_in_range(true, lt, gt, true),
            );
        }
        (Some(lt), _, _, Some(gte)) if lt > gte && (value < gte || value >= lt) => {
            push_i32(
                errors,
                field,
                validation::int32::Error::in_range(true, gte, lt, false),
            );
        }
        (Some(lt), _, _, Some(gte)) if lt <= gte && value >= lt && value < gte => {
            push_i32(
                errors,
                field,
                validation::int32::Error::not_in_range(true, lt, gte, false),
            );
        }
        (Some(lt), _, _, _) if value >= lt => {
            push_i32(errors, field, validation::int32::Error::Lt(lt));
        }
        (_, Some(lte), Some(gt), _) if lte > gt && (value <= gt || value > lte) => {
            push_i32(
                errors,
                field,
                validation::int32::Error::in_range(false, gt, lte, true),
            );
        }
        (_, Some(lte), Some(gt), _) if lte <= gt && value > lte && value <= gt => {
            push_i32(
                errors,
                field,
                validation::int32::Error::not_in_range(false, lte, gt, true),
            );
        }
        (_, Some(lte), _, Some(gte)) if lte > gte && (value < gte || value > lte) => {
            push_i32(
                errors,
                field,
                validation::int32::Error::in_range(true, gte, lte, true),
            );
        }
        (_, Some(lte), _, Some(gte)) if lte <= gte && value > lte && value < gte => {
            push_i32(
                errors,
                field,
                validation::int32::Error::not_in_range(false, lte, gte, false),
            );
        }
        (_, Some(lte), _, _) if value > lte => {
            push_i32(errors, field, validation::int32::Error::Lte(lte));
        }
        (_, _, Some(gt), _) if value <= gt => {
            push_i32(errors, field, validation::int32::Error::Gt(gt));
        }
        (_, _, _, Some(gte)) if value < gte => {
            push_i32(errors, field, validation::int32::Error::Gte(gte));
        }
        _ => {}
    }
    if !rules.r#in.is_empty() && !rules.r#in.contains(&value) {
        push_i32(
            errors,
            field,
            validation::int32::Error::In(rules.r#in.clone()),
        );
    }
    if rules.not_in.contains(&value) {
        push_i32(
            errors,
            field,
            validation::int32::Error::NotIn(rules.not_in.clone()),
        );
    }
}

fn push_i32(errors: &mut Vec<ValidationError>, field: &str, error: validation::int32::Error) {
    errors.push(ValidationError::new(field, validation::Error::Int32(error)));
}

fn validate_list(
    field: &FieldDescriptor,
    value: &Value,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    let Some(values) = value.as_list() else {
        return;
    };
    let list_rules = match &rules.r#type {
        Some(RuleType::Repeated(rules)) => Some(rules),
        _ => None,
    };
    if let Some(list_rules) = list_rules {
        if list_rules.ignore_empty() && values.is_empty() {
            return;
        }
        match (
            list_rules.min_items.map(to_usize),
            list_rules.max_items.map(to_usize),
        ) {
            (Some(minimum), Some(maximum)) if minimum == maximum && values.len() != minimum => {
                errors.push(ValidationError::new(
                    field.full_name(),
                    validation::Error::List(list::Error::MinItems(minimum)),
                ));
            }
            (minimum, maximum) => {
                if let Some(minimum) = minimum
                    && values.len() < minimum
                {
                    errors.push(ValidationError::new(
                        field.full_name(),
                        validation::Error::List(list::Error::MinItems(minimum)),
                    ));
                }
                if let Some(maximum) = maximum
                    && values.len() > maximum
                {
                    errors.push(ValidationError::new(
                        field.full_name(),
                        validation::Error::List(list::Error::MaxItems(maximum)),
                    ));
                }
            }
        }
        if list_rules.unique() && unique_count(values) != values.len() {
            errors.push(ValidationError::new(
                field.full_name(),
                validation::Error::List(list::Error::Unique),
            ));
        }
        if let Some(item_rules) = &list_rules.items {
            for (index, item) in values.iter().enumerate() {
                let mut item_errors = Vec::new();
                validate_list_item(field, item, item_rules, &mut item_errors);
                wrap_list_errors(field, index, item_errors, errors);
            }
        }
    }
    let skip_messages = list_rules
        .and_then(|rules| rules.items.as_ref())
        .as_ref()
        .and_then(|rules| rules.message)
        .is_some_and(|rules| rules.skip());
    validate_list_messages(field, value, skip_messages, errors);
}

fn validate_list_messages(
    field: &FieldDescriptor,
    value: &Value,
    skip: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some(values) = value.as_list() else {
        return;
    };
    if skip || !matches!(field.kind(), Kind::Message(_)) {
        return;
    }
    for (index, item) in values.iter().enumerate() {
        let Some(message) = item.as_message() else {
            continue;
        };
        let mut item_errors = Vec::new();
        validate_message(message, &mut item_errors);
        wrap_list_errors(field, index, item_errors, errors);
    }
}

fn validate_list_item(
    field: &FieldDescriptor,
    item: &Value,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    match (&field.kind(), &rules.r#type) {
        (Kind::String, Some(RuleType::String(string_rules))) => {
            validate_string(field.full_name(), item, string_rules, errors);
        }
        (Kind::Enum(descriptor), Some(RuleType::Enum(enum_rules))) => {
            validate_enum(field.full_name(), item, descriptor, enum_rules, errors);
        }
        _ => {}
    }
}

fn wrap_list_errors(
    field: &FieldDescriptor,
    index: usize,
    item_errors: Vec<ValidationError>,
    errors: &mut Vec<ValidationError>,
) {
    errors.extend(item_errors.into_iter().map(|error| {
        ValidationError::new(
            format!("{}[{index}]", field.full_name()),
            validation::Error::List(list::Error::Item(Box::new(error))),
        )
    }));
}

fn unique_count(values: &[Value]) -> usize {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<HashSet<_>>()
        .len()
}

fn validate_map(
    field: &FieldDescriptor,
    value: &Value,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    let Some(values) = value.as_map() else {
        return;
    };
    let map_rules = match &rules.r#type {
        Some(RuleType::Map(rules)) => Some(rules),
        _ => None,
    };
    if let Some(map_rules) = map_rules {
        if map_rules.ignore_empty() && values.is_empty() {
            return;
        }
        if let Some(minimum) = map_rules.min_pairs.map(to_usize)
            && values.len() < minimum
        {
            errors.push(ValidationError::new(
                field.full_name(),
                validation::Error::Map(map::Error::MinPairs(minimum)),
            ));
        }
        if let Some(maximum) = map_rules.max_pairs.map(to_usize)
            && values.len() > maximum
        {
            errors.push(ValidationError::new(
                field.full_name(),
                validation::Error::Map(map::Error::MaxPairs(maximum)),
            ));
        }
    }
    let Kind::Message(entry) = field.kind() else {
        return;
    };
    let value_field = entry.map_entry_value_field();
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    for (key, value) in entries {
        if let Some(key_rules) = map_rules.and_then(|rules| rules.keys.as_ref()) {
            let mut key_errors = Vec::new();
            validate_map_key(&map_path(field, key), key, key_rules, &mut key_errors);
            errors.extend(key_errors);
        }
        if let Some(value_rules) = map_rules.and_then(|rules| rules.values.as_ref()) {
            let mut value_errors = Vec::new();
            validate_list_item(&value_field, value, value_rules, &mut value_errors);
            wrap_map_errors(field, key, value_errors, errors);
        }
        let skip_messages = map_rules
            .and_then(|rules| rules.values.as_ref())
            .and_then(|rules| rules.message)
            .is_some_and(|rules| rules.skip());
        validate_map_message(field, key, value, &value_field, skip_messages, errors);
        if map_rules.is_some_and(|rules| rules.no_sparse()) && value.is_default(&value_field.kind())
        {
            errors.push(ValidationError::new(
                map_path(field, key),
                validation::Error::Map(map::Error::NoSparse),
            ));
        }
    }
}

fn validate_map_messages(
    field: &FieldDescriptor,
    value: &Value,
    skip: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some(values) = value.as_map() else {
        return;
    };
    let Kind::Message(entry) = field.kind() else {
        return;
    };
    let value_field = entry.map_entry_value_field();
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| *key);
    for (key, value) in entries {
        validate_map_message(field, key, value, &value_field, skip, errors);
    }
}

fn validate_map_message(
    field: &FieldDescriptor,
    key: &MapKey,
    value: &Value,
    value_field: &FieldDescriptor,
    skip: bool,
    errors: &mut Vec<ValidationError>,
) {
    if skip || !matches!(value_field.kind(), Kind::Message(_)) {
        return;
    }
    let Some(nested) = value.as_message() else {
        return;
    };
    let mut value_errors = Vec::new();
    validate_message(nested, &mut value_errors);
    wrap_map_errors(field, key, value_errors, errors);
}

fn validate_map_key(
    field: &str,
    key: &MapKey,
    rules: &FieldRules,
    errors: &mut Vec<ValidationError>,
) {
    if let (MapKey::String(value), Some(RuleType::String(string_rules))) = (key, &rules.r#type) {
        validate_string(field, &Value::String(value.clone()), string_rules, errors);
    }
}

fn wrap_map_errors(
    field: &FieldDescriptor,
    key: &MapKey,
    nested: Vec<ValidationError>,
    errors: &mut Vec<ValidationError>,
) {
    errors.extend(nested.into_iter().map(|error| {
        ValidationError::new(
            map_path(field, key),
            validation::Error::Map(map::Error::Values(Box::new(error))),
        )
    }));
}

fn map_path(field: &FieldDescriptor, key: &MapKey) -> String {
    match key {
        MapKey::String(value) => format!("{}[{value}]", field.full_name()),
        _ => format!("{}[{key:?}]", field.full_name()),
    }
}

fn validate_timestamp(
    field: &str,
    value: &DynamicMessage,
    rules: &TimestampRules,
    errors: &mut Vec<ValidationError>,
) {
    if !rules.lt_now() {
        return;
    }
    let Some(seconds_field) = value.descriptor().get_field_by_name("seconds") else {
        return;
    };
    let seconds = value.get_field(&seconds_field).as_i64().unwrap_or_default();
    let nanos = value
        .descriptor()
        .get_field_by_name("nanos")
        .and_then(|nanos_field| value.get_field(&nanos_field).as_i32())
        .unwrap_or_default();
    if let Some(cause) = invalid_timestamp_cause(seconds, nanos) {
        errors.push(ValidationError::new(
            field,
            validation::Error::InvalidRules(format!("{INVALID_TIMESTAMP_PREFIX}{cause}")),
        ));
        return;
    }
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok();
    let now_seconds = now
        .as_ref()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX);
    let now_nanos = now
        .as_ref()
        .and_then(|duration| i32::try_from(duration.subsec_nanos()).ok())
        .unwrap_or(i32::MAX);
    if (seconds, nanos) >= (now_seconds, now_nanos) {
        errors.push(ValidationError::new(
            field,
            validation::Error::Timestamp(timestamp::Error::LtNow),
        ));
    }
}

fn invalid_timestamp_cause(seconds: i64, nanos: i32) -> Option<String> {
    let value = protobuf_timestamp_fields(seconds, nanos);
    if seconds < MIN_TIMESTAMP_SECONDS {
        Some(format!("timestamp ({value}) before 0001-01-01"))
    } else if seconds > MAX_TIMESTAMP_SECONDS {
        Some(format!("timestamp ({value}) after 9999-12-31"))
    } else if !(0..1_000_000_000).contains(&nanos) {
        Some(format!("timestamp ({value}) has out-of-range nanos"))
    } else {
        None
    }
}

fn protobuf_timestamp_fields(seconds: i64, nanos: i32) -> String {
    let mut fields = Vec::with_capacity(2);
    if seconds != 0 {
        fields.push(format!("seconds:{seconds}"));
    }
    if nanos != 0 {
        fields.push(format!("nanos:{nanos}"));
    }
    fields.join(" ")
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use axum::body::Bytes;
    use openfga_proto::openfga::v1 as pb;
    use prost_reflect::ReflectMessage;

    use super::{WireCache, validate_all_cached, with_http_validation_key};

    #[test]
    fn test_should_cache_only_successful_exact_message_validation() {
        let cache = WireCache::new(NonZeroU64::new(1024 * 1024).unwrap_or(NonZeroU64::MIN));
        let valid = pb::ListStoresRequest {
            page_size: None,
            continuation_token: String::new(),
            name: String::new(),
        };
        assert!(validate_all_cached(&cache, &valid).is_empty());
        assert_eq!(cache.validated_message_count(), 1);
        assert!(validate_all_cached(&cache, &valid).is_empty());
        assert_eq!(cache.validated_message_count(), 1);

        let invalid = pb::ListStoresRequest {
            page_size: Some(pbjson_types::Int32Value { value: 0 }),
            continuation_token: String::new(),
            name: String::new(),
        };
        assert!(!validate_all_cached(&cache, &invalid).is_empty());
        assert!(!validate_all_cached(&cache, &invalid).is_empty());
        assert_eq!(cache.validated_message_count(), 1);
    }

    #[test]
    fn test_should_isolate_normalized_json_by_message_type_and_exact_bytes() {
        let cache = WireCache::new(NonZeroU64::new(1024 * 1024).unwrap_or(NonZeroU64::MIN));
        let list_descriptor = pb::ListStoresRequest::default().descriptor();
        let create_descriptor = pb::CreateStoreRequest::default().descriptor();
        let raw = Bytes::from_static(br#"{"name":null}"#);
        let normalized = Bytes::from_static(br"{}");
        cache.cache_normalized_json(&list_descriptor, raw.clone(), normalized.clone());

        assert_eq!(
            cache.normalized_json(&list_descriptor, &raw),
            Some(normalized)
        );
        assert!(cache.normalized_json(&create_descriptor, &raw).is_none());
        assert!(
            cache
                .normalized_json(&list_descriptor, &Bytes::from_static(br#"{"name": null}"#))
                .is_none()
        );
        let first_route = WireCache::http_validation_key(
            &list_descriptor,
            "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/check",
            raw.clone(),
        );
        let second_route = WireCache::http_validation_key(
            &list_descriptor,
            "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAW/check",
            raw,
        );
        assert_ne!(first_route.0, second_route.0);
    }

    #[tokio::test]
    async fn test_should_reuse_validation_only_for_the_exact_http_route_and_body() {
        let cache = WireCache::new(NonZeroU64::new(1024 * 1024).unwrap_or(NonZeroU64::MIN));
        let request = pb::GetStoreRequest {
            store_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        };
        let descriptor = request.descriptor();
        let body = Bytes::from_static(br"{}");
        let first_route = WireCache::http_validation_key(
            &descriptor,
            "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAV/check",
            body.clone(),
        );

        let errors = with_http_validation_key(first_route.clone(), async {
            validate_all_cached(&cache, &request)
        })
        .await;
        assert!(errors.is_empty());
        assert_eq!(cache.validated_message_count(), 1);

        let errors =
            with_http_validation_key(first_route, async { validate_all_cached(&cache, &request) })
                .await;
        assert!(errors.is_empty());
        assert_eq!(cache.validated_message_count(), 1);

        let second_route = WireCache::http_validation_key(
            &descriptor,
            "/stores/01ARZ3NDEKTSV4RRFFQ69G5FAW/check",
            body,
        );
        let errors = with_http_validation_key(second_route, async {
            validate_all_cached(&cache, &request)
        })
        .await;
        assert!(errors.is_empty());
        assert_eq!(cache.validated_message_count(), 2);
    }

    #[test]
    fn test_should_never_cache_time_dependent_validation() {
        let cache = WireCache::new(NonZeroU64::new(1024 * 1024).unwrap_or(NonZeroU64::MIN));
        let request = pb::ReadChangesRequest {
            store_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            r#type: String::new(),
            page_size: None,
            continuation_token: String::new(),
            start_time: Some(pbjson_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
        };

        assert!(validate_all_cached(&cache, &request).is_empty());
        assert!(validate_all_cached(&cache, &request).is_empty());
        assert_eq!(cache.validated_message_count(), 0);
    }
}
