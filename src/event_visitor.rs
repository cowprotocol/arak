// TODO:
// - Would be nicer as iterators.

use std::ops::Deref;

use solabi::value::{Value, ValueKind};

pub enum VisitKind<'a> {
    ArrayStart,
    ArrayEnd,
    Value(&'a ValueKind),
}

/// Visit all leaf and dynamic array values in the top level value in a deterministic order.
///
/// Does not visit tuples and fixed arrays because the inner values are treated in a flattened way.
///
/// Does not visit arrays because they are converted to the start and end enum kinds.
pub fn visit_kind<'a>(value: &'a ValueKind, visitor: &mut impl FnMut(VisitKind<'a>)) {
    match value {
        ValueKind::Tuple(values) => {
            for value in values {
                visit_kind(value, visitor);
            }
        }
        ValueKind::FixedArray(length, value) => {
            for _ in 0..*length {
                visit_kind(value, visitor);
            }
        }
        ValueKind::Array(value) => {
            visitor(VisitKind::ArrayStart);
            visit_kind(value, visitor);
            visitor(VisitKind::ArrayEnd);
        }
        value => visitor(VisitKind::Value(value)),
    }
}

pub enum VisitValue<'a> {
    ArrayStart,
    ArrayEnd,
    Value(&'a Value),
}

/// Like `visit` but for `Value` instead of `ValueKind`. Visit order is the same.
pub fn visit_value<'a>(value: &'a Value, visitor: &mut impl FnMut(VisitValue<'a>)) {
    match value {
        Value::Tuple(values) => {
            for value in values {
                visit_value(value, visitor);
            }
        }
        Value::FixedArray(array) => {
            for value in array.deref() {
                visit_value(value, visitor);
            }
        }
        Value::Array(array) => {
            visitor(VisitValue::ArrayStart);
            for value in array.deref() {
                visit_value(value, visitor);
            }
            visitor(VisitValue::ArrayEnd);
        }
        value => visitor(VisitValue::Value(value)),
    }
}
