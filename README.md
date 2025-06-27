# inc_orchestrator
Incubation repo for orchestration

## Getting started

### Using Cargo

[Install Rust](https://www.rust-lang.org/tools/install)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Build

You can either use manually usually `cargo` commands or use `xtask` approach

```bash
cargo xtask - print usage
```

The `xtask` has advantage that it builds using separate build dirs, so when building `test` and `target`, there is no need to rebuild each time.

##### Run some specific
Use regular commands but prefer `cargo xtask`

```bash
cargo xtask run --example basic
```

### Using Bazel
```bash
bazel run //orchestration:basic
```


### Bazel

#### Targets

##### Tests

Each component has defined test target as `component:tests` so You can run them via `bazel`:
```txt
bazel test //PATH_TO_COMPONENT:tests
```

You can also run all tests via:
```txt
bazel test //...
```
