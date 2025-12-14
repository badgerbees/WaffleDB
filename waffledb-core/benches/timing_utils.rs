/// Timing utilities for benchmarks
/// Provides correct throughput calculations without inf values

pub fn calc_throughput_ops_per_sec(operations: u64, elapsed_ns: f64) -> f64 {
    if elapsed_ns < 1000.0 {
        // Too fast to measure reliably
        return 0.0;
    }
    (operations as f64 / elapsed_ns) * 1_000_000_000.0
}

pub fn calc_throughput_from_ms(operations: u64, elapsed_ms: f64) -> f64 {
    if elapsed_ms < 0.1 {
        return 0.0;
    }
    (operations as f64 / elapsed_ms) * 1000.0
}

pub fn clamp_throughput(value: f64) -> f64 {
    // Prevent unrealistic values
    if value.is_infinite() || value.is_nan() {
        0.0
    } else {
        value.min(100_000_000_000.0) // Max realistic throughput
    }
}
