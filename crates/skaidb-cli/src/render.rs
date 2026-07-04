//! Result rendering shared by the network and embedded backends.

use skaidb_engine::{QueryOutput, ResultSet};
use skaidb_proto::Response;
use skaidb_types::Value;

/// Print a server `Response` from the binary protocol (network mode).
pub fn print_response(resp: &Response) {
    match resp {
        Response::Rows { columns, rows } => print_rows(columns, rows),
        Response::Mutation { affected } => {
            println!("OK, {affected} row(s) affected");
        }
        Response::Ddl => println!("OK"),
        // The driver maps errors to `Err`, so this arm is not normally hit.
        Response::Error(msg) => eprintln!("error: {msg}"),
        // The shell never issues Prepare; printed for completeness.
        Response::Prepared { id, params } => println!("PREPARED {id} ({params} params)"),
    }
}

/// Print a `QueryOutput` from the embedded engine (`--local` mode).
pub fn print_output(out: &QueryOutput) {
    match out {
        QueryOutput::Rows(rs) => print_result_set(rs),
        QueryOutput::Mutation { affected } => println!("OK, {affected} row(s) affected"),
        QueryOutput::Ddl => println!("OK"),
    }
}

fn print_result_set(rs: &ResultSet) {
    print_rows(&rs.columns, &rs.rows);
}

/// Render columns + rows as a simple aligned text table with a row count.
fn print_rows(columns: &[String], rows: &[Vec<Value>]) {
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    let rendered: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect();
    for row in &rendered {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", join_padded(columns, &widths));
    println!("{}", sep.join("-+-"));
    for row in &rendered {
        println!("{}", join_padded(row, &widths));
    }
    println!("({} row{})", rows.len(), if rows.len() == 1 { "" } else { "s" });
}

fn join_padded<S: AsRef<str>>(cells: &[S], widths: &[usize]) -> String {
    cells
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c.as_ref(), width = widths.get(i).copied().unwrap_or(0)))
        .collect::<Vec<_>>()
        .join(" | ")
}
