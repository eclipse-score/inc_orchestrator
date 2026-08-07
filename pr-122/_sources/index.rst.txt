..
   # *******************************************************************************
   # Copyright (c) 2024 Contributors to the Eclipse Foundation
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

Orchestration Framework
=============================

This documentation describes the **Orchestration** as declarative model to define cause-effect chains of actions

.. contents:: Table of Contents
   :depth: 2
   :local:

Overview
--------

This repository provides a standardized setup for projects using **C++** or **Rust** and **Bazel** as a build system.
It integrates best practices for build, test, CI/CD and documentation.

Requirements
------------

- TODO add linkage once it works

Quick Start
-----------

To build the module for host platform:

.. code-block:: bash

   bazel build //src/...

To build the module for QNX8:

.. code-block:: bash

   ./scripts/build_qnx8.sh

To run component tests:

.. code-block:: bash

   ./scripts/run_component_tests.sh
