/// Engine factory and type system.
/// 
/// Provides pluggable engine creation and selection logic.
/// Currently supports HNSW and Hybrid. Future: IVF, PQ, Custom.

pub mod hnsw_engine;
pub mod hybrid_engine;

use std::sync::Mutex;
pub use crate::engines::hnsw_engine::HNSWEngine;
pub use crate::engines::hybrid_engine::HybridEngine;
use waffledb_core::{VectorEngine, Result};

/// Engine type enumeration for engine selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineType {
    HNSW,
    Hybrid,
}

impl EngineType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EngineType::HNSW => "hnsw",
            EngineType::Hybrid => "hybrid",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "hnsw" => Some(EngineType::HNSW),
            "hybrid" => Some(EngineType::Hybrid),
            _ => None,
        }
    }
}

/// Create a new engine of the specified type.
pub fn create_engine(engine_type: EngineType) -> Result<Box<dyn VectorEngine>> {
    match engine_type {
        EngineType::HNSW => Ok(Box::new(HNSWEngine::default_config())),
        EngineType::Hybrid => Ok(Box::new(HybridEngine::new())),
    }
}

/// Thread-safe engine wrapper for concurrent access.
pub struct EngineWrapper {
    engine: Mutex<Box<dyn VectorEngine>>,
}

impl EngineWrapper {
    pub fn new(engine_type: EngineType) -> Result<Self> {
        let engine = create_engine(engine_type)?;
        Ok(EngineWrapper {
            engine: Mutex::new(engine),
        })
    }

    pub fn insert(&self, id: String, vector: waffledb_core::Vector) -> Result<()> {
        let mut engine = self.engine.lock().unwrap();
        engine.insert(id, vector)
    }

    pub fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<waffledb_core::EngineSearchResult>> {
        let engine = self.engine.lock().unwrap();
        engine.search(query, top_k)
    }

    pub fn delete(&self, id: &str) -> Result<()> {
        let mut engine = self.engine.lock().unwrap();
        engine.delete(id)
    }

    pub fn get(&self, id: &str) -> Option<waffledb_core::Vector> {
        let engine = self.engine.lock().unwrap();
        engine.get(id)
    }

    pub fn len(&self) -> usize {
        let engine = self.engine.lock().unwrap();
        engine.len()
    }

    pub fn stats(&self) -> waffledb_core::EngineStats {
        let engine = self.engine.lock().unwrap();
        engine.stats()
    }
}
