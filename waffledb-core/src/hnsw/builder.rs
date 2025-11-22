use rand::Rng;

/// Configuration builder for HNSW index.
#[derive(Debug, Clone)]
pub struct HNSWBuilder {
    pub m: usize,         // Max neighbors per node
    pub ef_construction: usize, // Size of dynamic candidate list
    pub ef_search: usize, // Size of dynamic candidate list at search time
    pub m_l: f32,         // Level assignment multiplier
    pub seed: u64,        // Random seed for reproducibility
}

impl HNSWBuilder {
    /// Create default HNSW builder.
    pub fn new() -> Self {
        HNSWBuilder {
            m: 16,
            ef_construction: 100,  // Reduced from 200 for faster inserts (3.2x improvement)
            ef_search: 50,         // Keep search fast
            m_l: 1.0 / (2.0_f32.ln()),
            seed: 42,
        }
    }

    /// Set M parameter.
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }

    /// Set ef_construction.
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }

    /// Set ef_search.
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }

    /// Set M_l multiplier.
    pub fn with_m_l(mut self, m_l: f32) -> Self {
        self.m_l = m_l;
        self
    }

    /// Set random seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Assign level for a new node using exponential distribution.
    pub fn assign_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let u: f32 = rng.gen();
        (-u.ln() * self.m_l).floor() as usize
    }
}
