# *******************************************************************************
# Copyright (c) 2025 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
# *******************************************************************************
load("@crates//:defs.bzl", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

_COMMON_DEPS = [
    "//src/foundation:libfoundation",
] + all_crate_deps(
    normal = True,
)

rust_library(
    name = "libasync_runtime",
    srcs = glob(["src/**/*.rs"]),
    crate_name = "async_runtime",
    visibility = ["//visibility:public"],
    deps = _COMMON_DEPS,
)

rust_library(
    name = "libasync_runtime_mocked",
    testonly = True,
    srcs = glob(["src/**/*.rs"]),
    crate_features = ["runtime-api-mock"],
    crate_name = "async_runtime",
    visibility = ["//visibility:public"],
    deps = _COMMON_DEPS,
)

rust_test(
    name = "tests",
    crate = ":libasync_runtime",
    deps = _COMMON_DEPS + [
        "//src/testing:test_utils",
    ],
)
