//! Event field iteration
//!
//! This module is for iterating over the fields of events in a deterministic way.

// TODO:
// - Would be nicer as iterators.

use solabi::{
    abi::Field,
    value::{Value, ValueKind},
};

/// The `&str` refers to the name of a field. It works in the following way:
/// - For named values it is the name . Examples: `bool my_bool`, `bool[] my_bools`
/// - For anonymous values it is empty. Example: `bool`
/// - For fixed arrays and dynamic arrays, the name of the array if forwarded into the inner types. When a tuple is reached the forwarded name is dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VisitKind<'a> {
    ArrayStart(&'a str),
    ArrayEnd,
    TupleStart(&'a str),
    TupleEnd,
    FixedArrayStart(usize, &'a str),
    FixedArrayEnd,
    Leaf(&'a ValueKind, &'a str),
}

pub fn visit_field<'a>(visitor: &mut impl FnMut(VisitKind<'a>), field: &'a Field) {
    visit_kind(
        visitor,
        &field.kind,
        &field.name,
        field.components.as_deref().unwrap_or_default(),
    );
}

pub fn visit_kind<'a>(
    visitor: &mut impl FnMut(VisitKind<'a>),
    kind: &'a ValueKind,
    name: &'a str,
    components: &'a [Field],
) {
    match kind {
        ValueKind::Tuple(values) => {
            visitor(VisitKind::TupleStart(name));
            assert_eq!(values.len(), components.len());
            for (kind, field) in values.iter().zip(components) {
                visit_kind(
                    visitor,
                    kind,
                    &field.name,
                    field.components.as_deref().unwrap_or_default(),
                );
            }
            visitor(VisitKind::TupleEnd);
        }
        ValueKind::FixedArray(length, kind) => {
            visitor(VisitKind::FixedArrayStart(*length, name));
            for _ in 0..*length {
                visit_kind(visitor, kind, name, components);
            }
            visitor(VisitKind::FixedArrayEnd);
        }
        ValueKind::Array(value) => {
            visitor(VisitKind::ArrayStart(name));
            visit_kind(visitor, value, name, components);
            visitor(VisitKind::ArrayEnd);
        }
        value => {
            assert!(components.is_empty());
            visitor(VisitKind::Leaf(value, name));
        }
    }
}

pub enum VisitValue<'a> {
    ArrayStart(usize),
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
            for value in array.as_slice() {
                visit_value(value, visitor);
            }
        }
        Value::Array(array) => {
            visitor(VisitValue::ArrayStart(array.len()));
            for value in array.as_slice() {
                visit_value(value, visitor);
            }
            visitor(VisitValue::ArrayEnd);
        }
        value => visitor(VisitValue::Value(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solabi::abi::EventDescriptor;

    fn parse_field(s: &str) -> Field {
        EventDescriptor::parse_declaration(&format!("event Event({s})"))
            .unwrap()
            .inputs
            .into_iter()
            .next()
            .unwrap()
            .field
    }

    fn collect_visits(field: &Field) -> Vec<VisitKind> {
        let mut visits = Vec::<VisitKind>::new();
        let mut visitor = |visit| {
            visits.push(visit);
        };
        visit_field(&mut visitor, field);
        visits
    }

    #[test]
    fn leaf() {
        let field = "bool b0";
        let field = dbg!(parse_field(field));
        let visits = collect_visits(&field);
        let expected = &[VisitKind::Leaf(&ValueKind::Bool, "b0")];
        assert_eq!(&visits, expected);
    }

    #[test]
    fn anonymous_tuple() {
        let field = "(bool b0, bool)";
        let field = dbg!(parse_field(field));
        let visits = collect_visits(&field);
        let expected = &[
            VisitKind::TupleStart(""),
            VisitKind::Leaf(&ValueKind::Bool, "b0"),
            VisitKind::Leaf(&ValueKind::Bool, ""),
            VisitKind::TupleEnd,
        ];
        assert_eq!(&visits, expected);
    }

    #[test]
    fn named_tuple() {
        let field = "(bool b0, bool) tuple0";
        let field = dbg!(parse_field(field));
        let visits = collect_visits(&field);
        let expected = &[
            VisitKind::TupleStart("tuple0"),
            VisitKind::Leaf(&ValueKind::Bool, "b0"),
            VisitKind::Leaf(&ValueKind::Bool, ""),
            VisitKind::TupleEnd,
        ];
        assert_eq!(&visits, expected);
    }

    #[test]
    fn fixed_array() {
        let field = "bool[2][3] a0";
        let field = dbg!(parse_field(field));
        let visits = collect_visits(&field);
        let expected = &[
            VisitKind::FixedArrayStart(3, "a0"),
            VisitKind::FixedArrayStart(2, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::FixedArrayEnd,
            VisitKind::FixedArrayStart(2, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::FixedArrayEnd,
            VisitKind::FixedArrayStart(2, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::Leaf(&ValueKind::Bool, "a0"),
            VisitKind::FixedArrayEnd,
            VisitKind::FixedArrayEnd,
        ];
        assert_eq!(&visits, expected);
    }

    #[test]
    fn complex() {
        let field = "(bool b0, (bool b1) t0)[1][1] a0";
        let field = dbg!(parse_field(field));
        let visits = collect_visits(&field);
        let expected = &[
            VisitKind::FixedArrayStart(1, "a0"),
            VisitKind::FixedArrayStart(1, "a0"),
            VisitKind::TupleStart("a0"),
            VisitKind::Leaf(&ValueKind::Bool, "b0"),
            VisitKind::TupleStart("t0"),
            VisitKind::Leaf(&ValueKind::Bool, "b1"),
            VisitKind::TupleEnd,
            VisitKind::TupleEnd,
            VisitKind::FixedArrayEnd,
            VisitKind::FixedArrayEnd,
        ];
        assert_eq!(&visits, expected);
    }
}
