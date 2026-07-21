//! CI-fast slice of the ACID crash harness: a few kill -9 rounds must show
//! ZERO hard violations (acked writes durable, acked commits fully
//! applied). Soft partial-visibility of UNacked transactions is documented
//! behavior (no commit record) and does not fail the run — see the
//! `acid-crash` binary for the full audit and QUERY_SYNTAX.md for the
//! written contract.

#[test]
fn crash_rounds_have_no_hard_violations() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_acid-crash"))
        .args(["--rounds", "4", "--seed", "99"])
        .status()
        .expect("run acid-crash");
    assert!(status.success(), "acid-crash reported HARD violations");
}
