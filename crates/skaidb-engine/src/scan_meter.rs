//! Per-statement scan metering: a thread-local budget on rows examined and a
//! wall-clock deadline, armed at statement entry and ticked from every row
//! decode/filter loop.
//!
//! Why thread-local: every execution surface runs one statement per thread
//! (REST connection threads, internode handler threads, the embedded
//! session), and the alternative — threading a meter through every `Cluster`
//! trait method — churns a dozen signatures for the same effect.
//!
//! Why it exists: `LIMIT n` bounds a query's *output*, not its *work*. A
//! predicate that matches (almost) nothing under an `ORDER BY .. LIMIT`
//! walks the entire table per cycle (a polling query did exactly that,
//! 2026-07-13, and OOM-looped production nodes), and a disconnected client's
//! statement used to keep executing to completion (zombie 60s-timeout
//! queries piled multi-GB gathers). The meter turns both into a bounded,
//! attributable error.

use std::cell::Cell;
use std::time::Instant;

use crate::error::EngineError;

thread_local! {
    static BUDGET: Cell<usize> = const { Cell::new(0) }; // 0 = unarmed
    static EXAMINED: Cell<usize> = const { Cell::new(0) };
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    static BYTE_BUDGET: Cell<usize> = const { Cell::new(0) }; // 0 = no byte budget
    static MATERIALIZED: Cell<usize> = const { Cell::new(0) };
}

/// Guard that disarms the meter when the statement finishes (any exit path).
#[derive(Debug)]
pub struct Armed(());

impl Drop for Armed {
    fn drop(&mut self) {
        BUDGET.with(|b| b.set(0));
        EXAMINED.with(|e| e.set(0));
        DEADLINE.with(|d| d.set(None));
        BYTE_BUDGET.with(|b| b.set(0));
        MATERIALIZED.with(|m| m.set(0));
    }
}

/// Arm the meter for the current thread's statement. `budget` rows examined
/// (`0` = no row budget), `byte_budget` bytes MATERIALIZED into a result set
/// (`0` = no byte budget — see [`tick_bytes`]), optional wall-clock `deadline`.
/// Nested arms keep the outermost meter: a statement's internal helpers must
/// not reset the count mid-flight.
pub fn arm(budget: usize, byte_budget: usize, deadline: Option<Instant>) -> Option<Armed> {
    if BUDGET.with(Cell::get) != 0
        || BYTE_BUDGET.with(Cell::get) != 0
        || DEADLINE.with(Cell::get).is_some()
    {
        return None; // already armed by an outer scope
    }
    if budget == 0 && byte_budget == 0 && deadline.is_none() {
        return None; // metering disabled
    }
    BUDGET.with(|b| b.set(budget));
    EXAMINED.with(|e| e.set(0));
    DEADLINE.with(|d| d.set(deadline));
    BYTE_BUDGET.with(|b| b.set(byte_budget));
    MATERIALIZED.with(|m| m.set(0));
    Some(Armed(()))
}

/// Record `bytes` RETAINED into the statement's result set; errors once the
/// running total exceeds the byte budget. This bounds MEMORY (a 250k-row scan
/// of multi-KB rows is within the row budget yet materializes GBs — the read
/// path that OOM-killed production coordinators). Ticked at result-push sites,
/// not the decode loop, so a streaming count/DISTINCT (which retains nothing)
/// is never charged.
#[inline]
pub fn tick_bytes(bytes: usize) -> Result<(), EngineError> {
    let budget = BYTE_BUDGET.with(Cell::get);
    if budget == 0 {
        return Ok(());
    }
    let total = MATERIALIZED.with(|m| {
        let v = m.get().saturating_add(bytes);
        m.set(v);
        v
    });
    if total > budget {
        return Err(EngineError::ResourceLimit(format!(
            "scan result exceeded {budget} bytes materialized: add a LIMIT, narrow the \
             projection/filter, or raise storage.scan_byte_budget"
        )));
    }
    Ok(())
}

/// Record `n` rows examined; errors once the statement exceeds its budget or
/// deadline. The deadline is checked once per ~1024 rows — a syscall-free
/// fast path for the common tick.
#[inline]
pub fn tick(n: usize) -> Result<(), EngineError> {
    let budget = BUDGET.with(Cell::get);
    let deadline = DEADLINE.with(Cell::get);
    if budget == 0 && deadline.is_none() {
        return Ok(());
    }
    let examined = EXAMINED.with(|e| {
        let v = e.get().saturating_add(n);
        e.set(v);
        v
    });
    if budget != 0 && examined > budget {
        return Err(EngineError::ResourceLimit(format!(
            "scan budget exceeded ({examined} rows examined): narrow the filter, add a \
             covering index, or raise storage.scan_row_budget"
        )));
    }
    if let Some(dl) = deadline {
        // Amortize the clock read.
        if examined % 1024 < n && Instant::now() > dl {
            return Err(EngineError::ResourceLimit(
                "statement timeout: exceeded storage.statement_timeout_secs".into(),
            ));
        }
    }
    Ok(())
}
