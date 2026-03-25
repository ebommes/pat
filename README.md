<p align="center">
  <img src="assets/logo.svg" alt="pat" width="400">
</p>

---

## Demo

<p align="center">
  <img src="assets/demo.gif" alt="pat demo" width="800">
</p>

## Installation

### Homebrew

```sh
brew tap ebommes/tap
brew install pat
```

### Cargo

```sh
cargo install --git https://github.com/ebommes/pat
```

### Binary download

Pre-built binaries for macOS and Linux (x86_64 and arm64) are available on the [releases page](https://github.com/ebommes/pat/releases).

## Usage

```
pat [OPTIONS] <FILES>...
```

By default, `pat` outputs CSV to stdout.

```sh
pat data.parquet
```

### Output formats

```sh
# CSV (default)
pat data.parquet

# Pretty ASCII table
pat --pretty data.parquet

# NDJSON (one JSON object per line, works with jq)
pat --json data.parquet | jq .
```

### Limit rows

```sh
# Show only the first 20 rows
pat -n 20 data.parquet
```

### Options

| Flag | Short | Description |
| ------ | ------- | ------------- |
| `--pretty` | `-p` | Output as a formatted ASCII table |
| `--json` | `-j` | Output as NDJSON (jq-compatible) |
| `--lines N` | `-n N` | Limit output to first N rows |

### Multiple files

```sh
pat file1.parquet file2.parquet
```

## Supported compression

pat supports Parquet files compressed with Snappy, Zstd, and LZ4.

## License

MIT
