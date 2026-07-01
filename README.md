<!--
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# OpenWhisk

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

:warning: **This project is a research prototype built for [CS525](https://courses.grainger.illinois.edu/cs525/sp2026/) at the University of Illinois - Urbana Champaign** :warning

WarmWhisk is a serverless functions platform built on top of [Apache OpenWhisk](https://github.com/apache/openwhisk) (v2.0.0.). It takes advantage of the WebAssembly (WASM) runtimes unique properties to enable low cold-start function execution and flexible scheduling. The purpose of this research project was to validate the potential of WASM in serverless situations, therefore, this project is not production-ready and requires enhancements and further testing.

Compared to OpenWhisk, WarmWhisk demonstrates a **40.8x** improvement in worst-case action latency during cold starts and a **77x** reduction in peak memory usage. However, post warm-up, OpenWhisk can execute functions 10% faster than WarmWhisk due to WASM runtime overhead. WarmWhisk also outperforms other WASM based serverless function platforms like SpinKube and WOW. Finally, we implement a data proximity scheduler which demonstrates that low cold-start times enable new shcheduling algorithms that can dramatically improve the performance of some workloads. 

You can read more about the project in this high-level blog post or this thorough paper.

## How to use

:warning: **Since this project is a prototype, expect bugs** :warning:

### Prerequisites
1. Clone the original [OpenWhisk](https://github.com/apache/openwhisk) repo and set up OpenWhisk with the [ansible](https://github.com/VishruthR/WarmWhisk/blob/9b722531125dcf24b3a0b9a7cecfbb7575f8b290/ansible/README.md) instructions. This will install prerequisites and ensure your machine works with regular OpenWhisk.

2. Build the WarmWhisk version of the [`wsk` cli tool](https://github.com/VishruthR/openwhisk-cli)

3. Install `docker`, `ansible`, `nodejs`

### Running WarmWhisk

You can edit [`group_vars`](ansible/environments/local/group_vars/all) for the `local` environment here. This will allow you to opt-into WASM execution and select the load balancer you wish to use. By default, the WASM invoker and DataProximityLoadBalancer are enabled.

If you are interested in a distributed setup, check out [these instructions](setup_ow/DistributedOWSetup.md)

```
# Set the environment
export ENVIRONMENT=local

cd OpenWhisk
./start_up_ow.sh

# Configure wsk CLI tool
wsk property set --apihost sp26-cs525-1820.cs.illinois.edu
wsk property set --auth '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'

# Test
wsk -i action create fib_wasm wasm_programs/fib.wasm --kind wasm:wasmtime --main _start
wsk -i action invoke fib_wasm --result --param n 10
```

[Instructions](metrics/README.md) for collecting metrics are here.

#### Data Dependencies

The `DataProximityLoadBalancer` routes function invocations to invokers that already have a dependency on disk. To specify a dependency, include the `--param data_dependency [filename]` option when running `wsk action invoke`.


