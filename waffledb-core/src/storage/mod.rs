pub mod rocksdb;
pub mod wal;
pub mod snapshot;
pub mod index_persistence;
pub mod crash_recovery;
pub mod batch_wal;
pub mod incremental_snapshots;
pub mod snapshot_repair;

pub use index_persistence::{
    IndexPersistence, IndexState, HNSWSnapshot, IndexMetadata, GraphLayer, StateFile,
};
pub use crash_recovery::{CrashRecoveryManager, RecoveryResult};
pub use batch_wal::{BatchWAL, BatchOp, BatchMetrics};
pub use incremental_snapshots::{IncrementalSnapshotManager, BaseSnapshot, DeltaSnapshot, IncrementalSnapshotMetadata, SnapshotType};
pub use snapshot_repair::{SnapshotRepairManager, VerifiedSnapshot, VerificationStatus, RepairResult};

#[cfg(test)]
mod tests;
