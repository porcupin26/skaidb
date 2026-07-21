//! Run every `.slt` file under `tests/corpus/` through the sqllogictest
//! runner, one fresh embedded database per file. Add a file (or a case to
//! an existing one) with every SQL bug fix — the corpus IS the regression
//! history, pg_regress-style.

use skaidb_slt::SkaiDb;

#[test]
fn corpus() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("corpus dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "empty corpus at {}", dir.display());
    let mut failures = Vec::new();
    for file in files {
        let mut runner = sqllogictest::Runner::new(|| async { Ok(SkaiDb::new()) });
        if let Err(e) = runner.run_file(&file) {
            failures.push(format!("{}: {e}", file.display()));
        }
    }
    assert!(failures.is_empty(), "corpus failures:\n{}", failures.join("\n"));
}
