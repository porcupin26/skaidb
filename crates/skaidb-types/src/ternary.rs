//! Three-valued logic (SQL `NULL` semantics, SPEC §2/§3).
//!
//! SQL boolean expressions evaluate to `True`, `False`, or `Unknown`; `Unknown`
//! arises whenever `NULL` participates in a comparison. `WHERE` keeps a row only
//! when its predicate is `True` (not `Unknown`).

/// A three-valued logic value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ternary {
    True,
    False,
    Unknown,
}

impl Ternary {
    /// Lift a definite boolean into ternary logic.
    pub fn from_bool(b: bool) -> Self {
        if b {
            Ternary::True
        } else {
            Ternary::False
        }
    }

    /// Lift an optional boolean: `None` (a `NULL` comparison) becomes `Unknown`.
    pub fn from_option(b: Option<bool>) -> Self {
        match b {
            Some(v) => Ternary::from_bool(v),
            None => Ternary::Unknown,
        }
    }

    /// SQL `AND`: `False` dominates, then `Unknown`, then `True`.
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Ternary::False, _) | (_, Ternary::False) => Ternary::False,
            (Ternary::True, Ternary::True) => Ternary::True,
            _ => Ternary::Unknown,
        }
    }

    /// SQL `OR`: `True` dominates, then `Unknown`, then `False`.
    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Ternary::True, _) | (_, Ternary::True) => Ternary::True,
            (Ternary::False, Ternary::False) => Ternary::False,
            _ => Ternary::Unknown,
        }
    }

    /// Whether a `WHERE`/`HAVING` predicate keeps the row: `True` only.
    pub fn is_true(self) -> bool {
        matches!(self, Ternary::True)
    }
}

/// SQL `NOT`: negates `True`/`False`, leaves `Unknown` unchanged.
impl std::ops::Not for Ternary {
    type Output = Ternary;

    fn not(self) -> Self {
        match self {
            Ternary::True => Ternary::False,
            Ternary::False => Ternary::True,
            Ternary::Unknown => Ternary::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ternary::*;

    #[test]
    fn not_rules() {
        assert_eq!(!True, False);
        assert_eq!(!False, True);
        assert_eq!(!Unknown, Unknown);
    }

    #[test]
    fn and_rules() {
        assert_eq!(True.and(True), True);
        assert_eq!(True.and(False), False);
        assert_eq!(False.and(Unknown), False);
        assert_eq!(True.and(Unknown), Unknown);
        assert_eq!(Unknown.and(Unknown), Unknown);
    }

    #[test]
    fn or_rules() {
        assert_eq!(False.or(False), False);
        assert_eq!(True.or(Unknown), True);
        assert_eq!(False.or(Unknown), Unknown);
        assert_eq!(Unknown.or(Unknown), Unknown);
    }

    #[test]
    fn where_keeps_only_true() {
        assert!(True.is_true());
        assert!(!False.is_true());
        assert!(!Unknown.is_true());
    }
}
