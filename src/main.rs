use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::Parser;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use url::Url;

#[derive(Parser)]
#[command(name = "pat", version, about = "cat for Parquet files")]
struct Cli {
    /// Parquet file(s) or URLs (s3://, gs://, az://) to display
    #[arg(required = true)]
    files: Vec<String>,

    /// Limit output to first N rows (per file)
    #[arg(short = 'n', long = "lines")]
    lines: Option<usize>,

    /// Output as NDJSON (one JSON object per line, jq-compatible)
    #[arg(short, long, conflicts_with = "pretty")]
    json: bool,

    /// Output as a formatted ASCII table
    #[arg(short, long, conflicts_with = "json")]
    pretty: bool,
}

enum InputLocation {
    Local(PathBuf),
    Remote(Url),
}

fn parse_location(s: &str) -> InputLocation {
    if let Ok(url) = Url::parse(s) {
        match url.scheme() {
            "s3" | "gs" | "az" | "abfss" => return InputLocation::Remote(url),
            _ => {}
        }
    }
    InputLocation::Local(PathBuf::from(s))
}

fn build_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let host = url.host_str().context("missing bucket/container in URL")?;
    let path = ObjectPath::from(url.path().trim_start_matches('/'));
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" => Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(host)
                .build()
                .context("failed to build S3 client")?,
        ),
        "gs" => Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(host)
                .build()
                .context("failed to build GCS client")?,
        ),
        "az" | "abfss" => {
            if url.username() != "" {
                anyhow::bail!(
                    "ABFSS URLs with account in host (abfss://fs@account.dfs...) \
                     are not supported; use az://container/path with \
                     AZURE_STORAGE_ACCOUNT_NAME set instead"
                );
            }
            Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_container_name(host)
                    .build()
                    .context("failed to build Azure client")?,
            )
        }
        scheme => anyhow::bail!("unsupported URL scheme: {scheme}"),
    };
    Ok((store, path))
}

async fn read_batches(path: &Path, limit: Option<usize>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet from {}", path.display()))?;
    if let Some(n) = limit {
        builder = builder.with_limit(n);
    }
    let reader = builder.build()?;
    let stream = futures::stream::iter(reader.map(|r| r.map_err(Into::into)));

    collect_batches(stream).await
}

async fn read_batches_remote(url: &Url, limit: Option<usize>) -> Result<Vec<RecordBatch>> {
    let (store, path) = build_store(url)?;
    let reader = ParquetObjectReader::new(store, path);
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("failed to read parquet from {url}"))?;
    if let Some(n) = limit {
        builder = builder.with_limit(n);
    }
    let stream = builder.build()?;

    collect_batches(stream.map(|r| r.map_err(Into::into))).await
}

async fn collect_batches(
    mut stream: impl futures::Stream<Item = Result<RecordBatch>> + Unpin,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

fn write_csv(batches: &[RecordBatch], write_header: bool) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let stdout = io::stdout();
    let mut writer = arrow_csv::WriterBuilder::new()
        .with_header(write_header)
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

fn run() -> Result<()> {
    let cli = Cli::parse();
    let limit = cli.lines;

    let locations: Vec<InputLocation> = cli.files.iter().map(|s| parse_location(s)).collect();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to create async runtime")?;

    for (i, loc) in locations.iter().enumerate() {
        let batches = match loc {
            InputLocation::Local(path) => runtime.block_on(read_batches(path, limit))?,
            InputLocation::Remote(url) => {
                runtime.block_on(read_batches_remote(url, limit))?
            }
        };

        if cli.json {
            write_json(&batches)?;
        } else if cli.pretty {
            write_pretty(&batches)?;
        } else {
            write_csv(&batches, i == 0)?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    if let Err(e) = run() {
        if let Some(io_err) = e.downcast_ref::<io::Error>()
            && io_err.kind() == io::ErrorKind::BrokenPipe
        {
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_location_local_path() {
        assert!(matches!(
            parse_location("data.parquet"),
            InputLocation::Local(_)
        ));
    }

    #[test]
    fn parse_location_relative_path() {
        assert!(matches!(
            parse_location("./dir/data.parquet"),
            InputLocation::Local(_)
        ));
    }

    #[test]
    fn parse_location_absolute_path() {
        assert!(matches!(
            parse_location("/tmp/data.parquet"),
            InputLocation::Local(_)
        ));
    }

    #[test]
    fn parse_location_s3_url() {
        let loc = parse_location("s3://my-bucket/path/to/file.parquet");
        assert!(matches!(loc, InputLocation::Remote(_)));
        if let InputLocation::Remote(url) = loc {
            assert_eq!(url.scheme(), "s3");
            assert_eq!(url.host_str(), Some("my-bucket"));
        }
    }

    #[test]
    fn parse_location_gs_url() {
        let loc = parse_location("gs://my-bucket/file.parquet");
        assert!(matches!(loc, InputLocation::Remote(_)));
        if let InputLocation::Remote(url) = loc {
            assert_eq!(url.scheme(), "gs");
        }
    }

    #[test]
    fn parse_location_az_url() {
        let loc = parse_location("az://my-container/file.parquet");
        assert!(matches!(loc, InputLocation::Remote(_)));
        if let InputLocation::Remote(url) = loc {
            assert_eq!(url.scheme(), "az");
        }
    }

    #[test]
    fn parse_location_abfss_url() {
        let loc = parse_location("abfss://my-container/file.parquet");
        assert!(matches!(loc, InputLocation::Remote(_)));
        if let InputLocation::Remote(url) = loc {
            assert_eq!(url.scheme(), "abfss");
        }
    }

    #[test]
    fn parse_location_http_falls_back_to_local() {
        assert!(matches!(
            parse_location("http://example.com/file.parquet"),
            InputLocation::Local(_)
        ));
    }

    #[test]
    fn build_store_missing_host() {
        let url = Url::parse("s3:///no-bucket").unwrap();
        assert!(build_store(&url).is_err());
    }

    #[test]
    fn build_store_unsupported_scheme() {
        let url = Url::parse("ftp://host/path").unwrap();
        assert!(build_store(&url).is_err());
    }

    #[test]
    fn build_store_abfss_with_account_in_host() {
        let url = Url::parse("abfss://fs@account.dfs.core.windows.net/path").unwrap();
        let err = build_store(&url).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected helpful error, got: {err}"
        );
    }

    fn make_test_parquet(num_rows: usize) -> NamedTempFile {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let values: Vec<i32> = (0..num_rows as i32).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tmp
    }

    #[test]
    fn read_batches_all_rows() {
        let tmp = make_test_parquet(100);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let batches = rt.block_on(read_batches(tmp.path(), None)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn read_batches_with_limit() {
        let tmp = make_test_parquet(100);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let batches = rt.block_on(read_batches(tmp.path(), Some(10))).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn read_batches_limit_exceeds_rows() {
        let tmp = make_test_parquet(5);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let batches = rt.block_on(read_batches(tmp.path(), Some(100))).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn read_batches_limit_zero() {
        let tmp = make_test_parquet(100);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let batches = rt.block_on(read_batches(tmp.path(), Some(0))).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn collect_batches_async_with_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let stream = futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(collect_batches(stream)).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }
}
