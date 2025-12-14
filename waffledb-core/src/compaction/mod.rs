/// Compaction Module: Background compaction of WriteBuffer → HNSW
///
/// Architecture:
/// - WriteBuffer accumulates vectors for O(1) inserts
/// - When full, background compaction merges into main HNSW index
/// - Incremental PQ codebook updates (don't need full rebuild)
/// - Vector deletion + TTL support for efficient cleanup
/// - No index rebuild required—just adds new vectors to graph

mod compaction_manager;
mod incremental_vectorizer;
mod tombstone_manager;
mod scheduler;
mod incremental_layer_builder;
mod compaction_tests;

pub use compaction_manager::{CompactionManager, CompactionConfig, CompactionStats, CompactionBatch};
pub use incremental_vectorizer::IncrementalVectorizer;
pub use tombstone_manager::{TombstoneManager, TombstoneEntry, TombstoneReason};
pub use scheduler::{CompactionScheduler, SchedulerConfig};
pub use incremental_layer_builder::{IncrementalLayerBuilder, LayerBuilderConfig, SubGraphLayer};
