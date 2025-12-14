use super::schema::Metadata;

/// Filter operators for metadata queries.
#[derive(Debug, Clone)]
pub enum FilterOp {
    Equals(String, String),
    Contains(String, String),
    Range(String, String, String),
}

impl FilterOp {
    /// Apply filter to metadata.
    pub fn matches(&self, metadata: &Metadata) -> bool {
        match self {
            FilterOp::Equals(key, value) => {
                metadata.get(key).map_or(false, |v| v == value)
            }
            FilterOp::Contains(key, substring) => {
                metadata
                    .get(key)
                    .map_or(false, |v| v.contains(substring.as_str()))
            }
            FilterOp::Range(key, start, end) => {
                if let Some(val) = metadata.get(key) {
                    (val as &str) >= (start as &str) && (val as &str) <= (end as &str)
                } else {
                    false
                }
            }
        }
    }
}

/// Filter builder.
#[derive(Debug, Clone, Default)]
pub struct FilterBuilder {
    conditions: Vec<FilterOp>,
}

impl FilterBuilder {
    pub fn new() -> Self {
        FilterBuilder {
            conditions: vec![],
        }
    }

    pub fn equals(mut self, key: String, value: String) -> Self {
        self.conditions.push(FilterOp::Equals(key, value));
        self
    }

    pub fn contains(mut self, key: String, substring: String) -> Self {
        self.conditions.push(FilterOp::Contains(key, substring));
        self
    }

    pub fn range(mut self, key: String, start: String, end: String) -> Self {
        self.conditions.push(FilterOp::Range(key, start, end));
        self
    }

    pub fn build(self) -> Filter {
        Filter {
            conditions: self.conditions,
        }
    }
}

/// Compiled filter.
#[derive(Debug, Clone)]
pub struct Filter {
    pub conditions: Vec<FilterOp>,
}

impl Filter {
    /// Apply filter to metadata (AND all conditions).
    pub fn matches(&self, metadata: &Metadata) -> bool {
        self.conditions.iter().all(|cond| cond.matches(metadata))
    }
}
