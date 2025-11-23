pub mod hybrid_search;
pub mod hybrid_search_engine;
pub mod filtered_search;
pub mod metadata_filtering;

pub use hybrid_search::{HybridSearch, HybridSearchResult, FusionWeights};
pub use hybrid_search_engine::{HybridSearchEngine, HybridSearchQuery, DetailedSearchResult};
pub use filtered_search::{FilteredSearchResult, search_with_filter, count_matching};
pub use metadata_filtering::{MetadataIndex, MetadataValue, FilterExpression};
