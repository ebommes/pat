use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Parser)]
#[command(name = "pat", version, about = "cat for Parquet files")]
struct Cli {
    /// Parquet file(s) to display
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Limit output to first N rows
    #[arg(short = 'n', long = "lines")]
    lines: Option<usize>,

    /// Output as NDJSON (one JSON object per line, jq-compatible)
    #[arg(short, long, conflicts_with = "pretty")]
    json: bool,

    /// Output as a formatted ASCII table
    #[arg(short, long)]
    pretty: bool,
}

fn read_batches(path: &PathBuf, limit: Option<usize>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet from {}", path.display()))?
        .build()?;

    let mut batches = Vec::new();
    let mut rows_remaining = limit;

    for batch_result in reader {
        let batch = batch_result?;
        match rows_remaining {
            Some(0) => break,
            Some(remaining) if batch.num_rows() > remaining => {
                batches.push(batch.slice(0, remaining));
                break;
            }
            Some(ref mut remaining) => {
                *remaining -= batch.num_rows();
                batches.push(batch);
            }
            None => batches.push(batch),
        }
    }

    Ok(batches)
}

fn write_csv(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let stdout = io::stdout();
    let mut writer = arrow_csv::WriterBuilder::new()
        .with_header(true)
        .build(stdout.lock());
    for batch in batches {
        writer.write(batch)?;
    }
    Ok(())
}

fn write_json(batches: &[RecordBatch]) -> Result<()> {
    let stdout = io::stdout();
    let mut writer = arrow_json::LineDelimitedWriter::new(stdout.lock());
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

fn write_pretty(batches: &[RecordBatch]) -> Result<()> {
    print_batches(batches)?;
    io::stdout().flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let limit = cli.lines;

    for path in &cli.files {
        let batches = read_batches(path, limit)?;
        if cli.json {
            write_json(&batches)?;
        } else if cli.pretty {
            write_pretty(&batches)?;
        } else {
            write_csv(&batches)?;
        }
    }

    Ok(())
}
