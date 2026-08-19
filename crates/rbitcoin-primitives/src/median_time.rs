//! BIP113 median-time-past of an already-collected timestamp window.

/// Median of `times` (unsorted OK). `times` must be non-empty.
#[must_use]
pub fn median_time_past_times(times: &[u32]) -> u32 {
    debug_assert!(!times.is_empty());
    let mut sorted = times.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsorted_three() {
        assert_eq!(median_time_past_times(&[3, 1, 2]), 2);
    }
}
