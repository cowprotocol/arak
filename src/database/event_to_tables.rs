//! Conversion of `EventDescriptor` into SQL schema
//!
//! This module turns an `EventDescriptor` into an SQL schema by providing table and column names. How `ValueKind`s are mapped to SQL types is up to the specific database.

use anyhow::{anyhow, Result};
use solabi::{
    abi::{EventDescriptor, Field},
    ValueKind,
};

use super::{
    event_visitor::{visit_field, VisitKind},
    keywords::KEYWORDS,
};

#[derive(Debug, Eq, PartialEq)]
pub struct Tables<'a> {
    pub primary: Table<'a>,
    pub dynamic_arrays: Vec<Table<'a>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Table<'a> {
    /// The table name includes a sanitized version of the original event name.
    pub name: String,
    pub columns: Vec<Column<'a>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Column<'a> {
    // leaf kind
    pub kind: &'a ValueKind,
    pub name: String,
}

pub fn event_to_tables<'a>(name: &str, event: &'a EventDescriptor) -> Result<Tables<'a>> {
    // TODO:
    // - Handle indexed fields.

    // To avoid later confusion, force the user provided event name to be valid without change.
    let sanitized = sanitize_name(name);
    if sanitized != name {
        return Err(anyhow!(
            "Event name '{name}' is not valid. Try '{sanitized}'."
        ));
    }
    let name = sanitized;

    // The database reserves tables starting with underscore for internal use. A user provided event table name could conflict with that.
    if name.starts_with('_') {
        return Err(anyhow!(
            "Event '{name}' starts with an underscore, which isn't allowed."
        ));
    }

    // Nested dynamic arrays are rare and hard to handle. The recursive visiting code and SQL schema becomes more complicated. Handle this properly later.
    for input in &event.inputs {
        if has_nested_dynamic_arrays(&input.field) {
            return Err(anyhow!(
                "Event contains a dynamic array inside of a dynamic array. This isn't supported."
            ));
        }
    }

    let mut primary = Table {
        name: name.clone(),
        columns: Default::default(),
    };
    let mut dynamic_arrays = Vec::new();
    for input in &event.inputs {
        handle_field_simple_names(&name, &mut primary, &mut dynamic_arrays, &input.field);
    }
    Ok(Tables {
        primary,
        dynamic_arrays,
    })
}

fn has_nested_dynamic_arrays(field: &Field) -> bool {
    let mut level: u32 = 0;
    let mut max_level: u32 = 0;
    let mut visitor = |visit: VisitKind| match visit {
        VisitKind::ArrayStart(_) => {
            level += 1;
            max_level = std::cmp::max(max_level, level);
        }
        VisitKind::ArrayEnd => level -= 1,
        VisitKind::TupleStart(_)
        | VisitKind::TupleEnd
        | VisitKind::FixedArrayStart(..)
        | VisitKind::FixedArrayEnd
        | VisitKind::Leaf(..) => (),
    };
    visit_field(&mut visitor, field);
    max_level > 1
}

fn handle_field_simple_names<'a>(
    event_name: &str,
    primary: &mut Table<'a>,
    dynamic_arrays: &mut Vec<Table<'a>>,
    field: &'a Field,
) {
    let mut dynamic_array: Option<usize> = None;
    let mut visitor = move |value: VisitKind<'a>| match value {
        VisitKind::ArrayStart(name) => {
            let index = dynamic_arrays.len();
            dynamic_array = Some(index);
            let name = if name.is_empty() { "array" } else { name };
            dynamic_arrays.push(Table {
                name: sanitize_name(&format!("{event_name}_{name}_{index}")),
                columns: Default::default(),
            });
        }
        VisitKind::ArrayEnd => {
            dynamic_array = None;
        }
        VisitKind::Leaf(kind, name) => {
            let table = match dynamic_array {
                Some(index) => &mut dynamic_arrays[index],
                None => primary,
            };
            let name = if name.is_empty() { "field" } else { name };
            table.columns.push(Column {
                kind,
                name: sanitize_name(&format!("{name}_{}", table.columns.len())),
            });
        }
        _ => (),
    };
    visit_field(&mut visitor, field);
}

fn sanitize_name(name: &str) -> String {
    let sanitized = sanitize_name_(name);
    assert_eq!(sanitize_name_(&sanitized), sanitized);
    sanitized
}

fn sanitize_name_(name: &str) -> String {
    let is_allowed_character = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut result: String = name.chars().filter(|c| is_allowed_character(*c)).collect();
    if result.is_empty() || !is_allowed_character(result.chars().next().unwrap()) {
        result.insert(0, '_');
    }
    let is_keyword = |s: &str| {
        let lowercase = s.to_ascii_lowercase();
        KEYWORDS.iter().any(|word| *word == lowercase)
    };
    if is_keyword(&result) {
        result.push('_');
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use ValueKind as VK;

    /// Helper type to make expected tables struct terser to define in tests.
    type TestTables<'a> = &'a [(&'a str, &'a [(&'a ValueKind, &'a str)])];

    fn tables(tables: TestTables<'_>) -> Tables<'_> {
        fn columns<'a>(columns: &'a [(&'a VK, &'a str)]) -> Vec<Column<'a>> {
            columns
                .iter()
                .map(|(kind, name)| Column {
                    kind,
                    name: name.to_string(),
                })
                .collect()
        }

        let primary = tables[0];
        let rest = &tables[1..];
        Tables {
            primary: Table {
                name: primary.0.to_string(),
                columns: columns(primary.1),
            },
            dynamic_arrays: rest
                .iter()
                .map(|(table_name, columns_)| Table {
                    name: table_name.to_string(),
                    columns: columns(columns_),
                })
                .collect(),
        }
    }

    fn assert_tables(event: &str, expected: TestTables) {
        let expected = tables(expected);
        let event = EventDescriptor::parse_declaration(event).unwrap();
        let tables = event_to_tables("event", &event).unwrap();
        assert_eq!(
            tables, expected,
            "actual: {tables:#?} !=\nexpected: {expected:#?}"
        );
    }

    #[test]
    fn with_anonymous() {
        let event = r#"
event Event(
    bool b0,
    bool,
    address a0,
    bool
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b0_0"),
                (&VK::Bool, "field_1"),
                (&VK::Address, "a0_2"),
                (&VK::Bool, "field_3"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn anonymous_tuple() {
        let event = r#"
event Event(
    bool b0,
    (
        bool b0,
        bool b1
    ),
    address a0
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b0_0"),
                (&VK::Bool, "b0_1"),
                (&VK::Bool, "b1_2"),
                (&VK::Address, "a0_3"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn named_tuple() {
        let event = r#"
event Event(
    bool b0,
    (
        bool b0,
        bool b1
    ) my_bools,
    address a0
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b0_0"),
                (&VK::Bool, "b0_1"),
                (&VK::Bool, "b1_2"),
                (&VK::Address, "a0_3"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn fixed_array() {
        let event = r#"
event Event(
    bool b0,
    bool b1,
    bool[2] foo,
    address a0
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b0_0"),
                (&VK::Bool, "b1_1"),
                (&VK::Bool, "foo_2"),
                (&VK::Bool, "foo_3"),
                (&VK::Address, "a0_4"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn dynamic_array() {
        let event = r#"
event Event(
    bool b0,
    bool[] foo,
    address a0
  )
"#;
        let expected: TestTables = &[
            ("event", &[(&VK::Bool, "b0_0"), (&VK::Address, "a0_1")]),
            ("event_foo_0", &[(&VK::Bool, "foo_0")]),
        ];
        assert_tables(event, expected);
    }

    #[test]
    fn dynamic_array_tuple() {
        let event = r#"
event Event(
    bool b0,
    (bool bar)[] foo,
    address a0
  )
"#;
        let expected: TestTables = &[
            ("event", &[(&VK::Bool, "b0_0"), (&VK::Address, "a0_1")]),
            ("event_foo_0", &[(&VK::Bool, "bar_0")]),
        ];
        assert_tables(event, expected);
    }

    #[test]
    fn nested_tuples() {
        let event = r#"
event Event(
    bool b0,
    (
        bool b0,
        (
            bool,
            bool
        ) inner,
        bool b1
    ) outer
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b0_0"),
                (&VK::Bool, "b0_1"),
                (&VK::Bool, "field_2"),
                (&VK::Bool, "field_3"),
                (&VK::Bool, "b1_4"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn nested_fixed_array() {
        let event = r#"
event Event(
    bool[2][3] b
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "b_0"),
                (&VK::Bool, "b_1"),
                (&VK::Bool, "b_2"),
                (&VK::Bool, "b_3"),
                (&VK::Bool, "b_4"),
                (&VK::Bool, "b_5"),
            ],
        )];
        assert_tables(event, expected);
    }

    #[test]
    fn nested_fixed_array_and_tuple() {
        let event = r#"
event Event(
    (bool, bool b1, (bool b2) inner_tuple)[1][2] array0
  )
"#;
        let expected: TestTables = &[(
            "event",
            &[
                (&VK::Bool, "field_0"),
                (&VK::Bool, "b1_1"),
                (&VK::Bool, "b2_2"),
                (&VK::Bool, "field_3"),
                (&VK::Bool, "b1_4"),
                (&VK::Bool, "b2_5"),
            ],
        )];
        assert_tables(event, expected);
    }
}
