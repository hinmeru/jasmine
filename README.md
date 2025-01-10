![j*](icon.png)

# jasmine

[![Release Python](https://github.com/hinmeru/jasmine/actions/workflows/release-python.yml/badge.svg)](https://github.com/hinmeru/jasmine/actions/workflows/release-python.yml)

Jasmine is a spec of analytics programming language, inspired by q, rust, python etc programming languages.

Jasminum is a Rust + Python implementation of Jasmine engine powered by [polars](https://pola.rs/)

## Future Roadmap

- [x] IPC with jasmine, duckdb, other databases etc.
- [x] timer
- [x] running source code file (with `-f` option, `.jsm` extension)
- [ ] `import` function
- [ ] `argparse` function
- [ ] jupyter kernel
- [ ] node-jasmine
- [ ] vscode plugin
  - [x] syntax highlighting
  - [ ] grid and chart visualization
  - [ ] language server
- [ ] all built-in functions
- [ ] `pub-sub-replay` feature
  - [ ] `pub-sub`
  - [ ] `replay`, file1 byte size of each message, file2 byte of each message
- [ ] performance sensitive operations in Rust
- [ ] high-performance Rust engine

## References

- [polars](https://pola.rs/), Blazingly fast DataFrames in Rust
- [pest](https://pest.rs/), The Elegant Parser
- [pyo3](https://pyo3.rs/), Rust bindings for Python
- [Apache Arrow](https://arrow.apache.org/), Platform for in-memory Analytics
