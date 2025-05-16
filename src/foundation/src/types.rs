// Copyright (c) 2025 Qorix GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Apache License, Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: Apache-2.0
//

#[derive(PartialEq, Debug)]
pub enum CommonErrors {
    NoData,
    AlreadyDone,
    GenericError,
    OperationAborted,
    Panicked, // panic! was handled
    Timeout,
}
