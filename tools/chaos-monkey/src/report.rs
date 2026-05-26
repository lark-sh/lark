//! Structured logging and violation tracking for chaos testing.
//!
//! Accumulates results across cycles and produces a final summary.

use crate::compaction::CompactionResult;
use crate::verify::VerificationResult;
use std::time::{Duration, Instant};
use tracing::{error, info};

/// Tracks results across all chaos cycles.
pub struct ChaosReport {
    start_time: Instant,
    cycles: Vec<CycleReport>,
    total_violations: usize,
}

/// Report for a single chaos cycle.
pub struct CycleReport {
    pub cycle_number: usize,
    pub kill_strategy: String,
    pub operations_sent: usize,
    pub committed: usize,
    pub rejected: usize,
    pub pending: usize,
    /// Pre-kill verification: how many violations found while server was still running
    pub pre_kill_violations: usize,
    /// Pre-kill verification: how many paths were checked
    pub pre_kill_paths_checked: usize,
    pub compaction: CompactionResult,
    pub verification: VerificationResult,
    pub duration: Duration,
}

impl ChaosReport {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            cycles: Vec::new(),
            total_violations: 0,
        }
    }

    /// Record a completed cycle.
    pub fn record_cycle(&mut self, report: CycleReport) {
        // Pre-kill violations are real bugs (live server returning wrong data),
        // so they must gate the cycle's FAILED branch and the run-level exit
        // code just like post-restart violations.
        let violations = report.verification.violation_count() + report.pre_kill_violations;
        self.total_violations += violations;

        if violations > 0 {
            error!(
                "CYCLE {} FAILED: {} post-restart violations, {} pre-kill violations (strategy: {}, ops: {}, committed: {}, pending: {})",
                report.cycle_number,
                violations,
                report.pre_kill_violations,
                report.kill_strategy,
                report.operations_sent,
                report.committed,
                report.pending,
            );

            for v in &report.verification.missing_committed {
                error!(
                    "  MISSING committed: {} (expected {}, ACK {:.1}s before kill)",
                    v.path,
                    v.expected_type,
                    v.ack_age_secs.unwrap_or(-1.0),
                );
            }
            for v in &report.verification.wrong_values {
                error!(
                    "  WRONG value: {} (expected {}, got {}, ACK {:.1}s before kill)",
                    v.path,
                    v.expected_type,
                    v.actual_type,
                    v.ack_age_secs.unwrap_or(-1.0),
                );
            }
            for err in &report.verification.disk.wal_errors {
                error!("  WAL error: {}", err);
            }
            if let Some(ref err) = report.verification.disk.blob_error {
                error!("  Blob error: {}", err);
            }
            if let Some(ref err) = report.verification.disk.sequence_error {
                error!("  Sequence error: {}", err);
            }
            if let Some(ref err) = report.compaction.error {
                error!("  Compaction error: {}", err);
            }
        } else {
            info!(
                "CYCLE {} OK: strategy={}, ops={}, committed={}, rejected={}, pending={}, pre_kill={}/{} ok, post_restart={}/{} ok, compact=[{}], disk=[{}] ({:.1}s)",
                report.cycle_number,
                report.kill_strategy,
                report.operations_sent,
                report.committed,
                report.rejected,
                report.pending,
                report.pre_kill_paths_checked,
                report.pre_kill_paths_checked, // all OK since violations == 0
                report.verification.paths_checked,
                report.verification.paths_checked,
                report.compaction.summary(),
                report.verification.disk.summary(),
                report.duration.as_secs_f64(),
            );
        }

        self.cycles.push(report);
    }

    /// Print the final summary.
    pub fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total_ops: usize = self.cycles.iter().map(|c| c.operations_sent).sum();
        let total_committed: usize = self.cycles.iter().map(|c| c.committed).sum();
        let total_paths_verified: usize = self
            .cycles
            .iter()
            .map(|c| c.verification.paths_checked)
            .sum();

        info!("========================================");
        info!("CHAOS MONKEY FINAL REPORT");
        info!("========================================");
        info!("Duration:          {:.1}s", elapsed.as_secs_f64());
        info!("Cycles completed:  {}", self.cycles.len());
        info!("Total operations:  {}", total_ops);
        info!("Total committed:   {}", total_committed);
        info!("Paths verified:    {}", total_paths_verified);
        info!("Total violations:  {}", self.total_violations);
        info!("========================================");

        if self.total_violations > 0 {
            error!(
                "RESULT: FAILED ({} violations across {} cycles)",
                self.total_violations,
                self.cycles
                    .iter()
                    .filter(|c| c.verification.has_violations() || c.pre_kill_violations > 0)
                    .count()
            );
        } else {
            info!(
                "RESULT: PASSED (0 violations across {} cycles)",
                self.cycles.len()
            );
        }
    }

    /// Returns true if any violations were detected.
    pub fn has_violations(&self) -> bool {
        self.total_violations > 0
    }

    /// Total cycles completed.
    pub fn cycles_completed(&self) -> usize {
        self.cycles.len()
    }
}
