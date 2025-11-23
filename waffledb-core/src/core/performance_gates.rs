/// Performance Baseline & Gate System for WaffleDB
/// 
/// Establishes performance thresholds that CI will enforce.
/// Any regression > threshold fails CI automatically.
/// 
/// Use: cargo bench --bench benchmark_performance_gates -- --baseline

use std::time::Instant;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct PerformanceBaseline {
    /// Insert throughput (vectors/second)
    pub insert_throughput: f64,
    
    /// Insert latency P50 (microseconds)
    pub insert_latency_p50_us: f64,
    
    /// Insert latency P99 (microseconds)
    pub insert_latency_p99_us: f64,
    
    /// Search latency P50 (milliseconds)
    pub search_latency_p50_ms: f64,
    
    /// Search latency P99 (milliseconds)
    pub search_latency_p99_ms: f64,
    
    /// Memory per vector (bytes)
    pub memory_per_vector: f64,
    
    /// Search recall (0.0-1.0)
    pub search_recall: f64,
}

impl PerformanceBaseline {
    /// Baseline for 1M vectors with 384 dimensions
    /// These are REALISTIC targets based on hybrid architecture
    pub fn production_1m() -> Self {
        PerformanceBaseline {
            // Hybrid insert: 25-35K vectors/sec (with buffer + periodic HNSW builds)
            insert_throughput: 25_000.0,
            
            // Insert latency P50: 30-50 microseconds (amortized)
            insert_latency_p50_us: 40.0,
            
            // Insert latency P99: 100-200 microseconds (includes occasional builds)
            insert_latency_p99_us: 150.0,
            
            // Search latency P50: 2-3 milliseconds (HNSW layer descent)
            search_latency_p50_ms: 2.5,
            
            // Search latency P99: 5-8 milliseconds (includes cache misses)
            search_latency_p99_ms: 7.0,
            
            // Memory: ~190-220 bytes/vector (HNSW graph + metadata)
            memory_per_vector: 210.0,
            
            // Search recall: >0.92 with ef_search=50
            search_recall: 0.92,
        }
    }
    
    /// Baseline for small scale (10K vectors)
    pub fn development_10k() -> Self {
        PerformanceBaseline {
            insert_throughput: 30_000.0,
            insert_latency_p50_us: 30.0,
            insert_latency_p99_us: 100.0,
            search_latency_p50_ms: 0.5,
            search_latency_p99_ms: 1.0,
            memory_per_vector: 200.0,
            search_recall: 0.95,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceGates {
    /// Maximum allowed regression: 10%
    pub max_regression_percent: f64,
    
    /// Minimum required improvement for opt features: 5%
    pub min_improvement_percent: f64,
    
    /// Thresholds per metric
    pub gates: HashMap<String, f64>,
}

impl PerformanceGates {
    pub fn default() -> Self {
        let mut gates = HashMap::new();
        
        // If throughput drops below this, fail CI
        gates.insert("insert_throughput_min".to_string(), 22_500.0); // 10% below baseline
        
        // If latency increases above this, fail CI
        gates.insert("insert_latency_p99_max_us".to_string(), 165.0); // 10% above baseline
        gates.insert("search_latency_p99_max_ms".to_string(), 7.7); // 10% above baseline
        
        // If memory increases above this, fail CI
        gates.insert("memory_per_vector_max".to_string(), 231.0); // 10% above baseline
        
        // If recall drops below this, fail CI
        gates.insert("search_recall_min".to_string(), 0.87); // 5% below baseline
        
        PerformanceGates {
            max_regression_percent: 10.0,
            min_improvement_percent: 5.0,
            gates,
        }
    }
    
    /// Check if measurements pass all gates
    pub fn check(
        &self,
        insert_throughput: f64,
        insert_latency_p99: f64,
        search_latency_p99: f64,
        memory_per_vector: f64,
        search_recall: f64,
    ) -> Vec<String> {
        let mut failures = Vec::new();
        
        if insert_throughput < self.gates["insert_throughput_min"] {
            failures.push(format!(
                "❌ INSERT THROUGHPUT REGRESSION: {:.0}/s (min: {:.0}/s)",
                insert_throughput, self.gates["insert_throughput_min"]
            ));
        }
        
        if insert_latency_p99 > self.gates["insert_latency_p99_max_us"] {
            failures.push(format!(
                "❌ INSERT LATENCY P99 REGRESSION: {:.1}μs (max: {:.1}μs)",
                insert_latency_p99, self.gates["insert_latency_p99_max_us"]
            ));
        }
        
        if search_latency_p99 > self.gates["search_latency_p99_max_ms"] {
            failures.push(format!(
                "❌ SEARCH LATENCY P99 REGRESSION: {:.2}ms (max: {:.2}ms)",
                search_latency_p99, self.gates["search_latency_p99_max_ms"]
            ));
        }
        
        if memory_per_vector > self.gates["memory_per_vector_max"] {
            failures.push(format!(
                "❌ MEMORY PER VECTOR REGRESSION: {:.1} bytes (max: {:.1} bytes)",
                memory_per_vector, self.gates["memory_per_vector_max"]
            ));
        }
        
        if search_recall < self.gates["search_recall_min"] {
            failures.push(format!(
                "❌ SEARCH RECALL REGRESSION: {:.3} (min: {:.3})",
                search_recall, self.gates["search_recall_min"]
            ));
        }
        
        failures
    }
}

/// Measure latency with nanosecond precision
pub struct LatencyMeasurement {
    latencies: Vec<u64>, // nanoseconds
}

impl LatencyMeasurement {
    pub fn new() -> Self {
        LatencyMeasurement {
            latencies: Vec::new(),
        }
    }
    
    pub fn measure<F>(&mut self, f: F)
    where
        F: FnOnce(),
    {
        let start = Instant::now();
        f();
        let elapsed = start.elapsed().as_nanos() as u64;
        self.latencies.push(elapsed);
    }
    
    pub fn p50_us(&self) -> f64 {
        let idx = self.latencies.len() / 2;
        let mut sorted = self.latencies.clone();
        sorted.sort();
        sorted[idx] as f64 / 1_000.0
    }
    
    pub fn p99_us(&self) -> f64 {
        let idx = (self.latencies.len() as f64 * 0.99).ceil() as usize;
        let mut sorted = self.latencies.clone();
        sorted.sort();
        sorted[idx.min(sorted.len() - 1)] as f64 / 1_000.0
    }
    
    pub fn p99_ms(&self) -> f64 {
        self.p99_us() / 1_000.0
    }
    
    pub fn mean_us(&self) -> f64 {
        let sum: u64 = self.latencies.iter().sum();
        sum as f64 / self.latencies.len() as f64 / 1_000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_performance_gates_initialization() {
        let gates = PerformanceGates::default();
        assert_eq!(gates.max_regression_percent, 10.0);
        assert!(gates.gates.contains_key("insert_throughput_min"));
    }
    
    #[test]
    fn test_latency_measurement() {
        let mut measurement = LatencyMeasurement::new();
        for _ in 0..100 {
            measurement.measure(|| {
                std::thread::sleep(std::time::Duration::from_micros(1));
            });
        }
        
        let p50 = measurement.p50_us();
        let p99 = measurement.p99_us();
        
        // Should be at least 1 microsecond
        assert!(p50 >= 1.0);
        assert!(p99 >= p50);
    }
    
    #[test]
    fn test_gates_pass_baseline() {
        let gates = PerformanceGates::default();
        let baseline = PerformanceBaseline::production_1m();
        
        let failures = gates.check(
            baseline.insert_throughput,
            baseline.insert_latency_p99_us,
            baseline.search_latency_p99_ms,
            baseline.memory_per_vector,
            baseline.search_recall,
        );
        
        // Baseline should pass its own gates
        assert_eq!(failures.len(), 0, "Baseline failed gates: {:?}", failures);
    }
    
    #[test]
    fn test_gates_fail_regression() {
        let gates = PerformanceGates::default();
        
        // Test with 15% throughput regression (should fail)
        let failures = gates.check(
            22_000.0, // Below minimum
            150.0,
            7.0,
            210.0,
            0.92,
        );
        
        assert!(!failures.is_empty(), "Should catch throughput regression");
        assert!(failures[0].contains("INSERT THROUGHPUT"));
    }
}
