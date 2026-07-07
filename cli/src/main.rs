// Copyright 2024-2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use jj_cli::cli_util::CliRunner;
use jj_lib::repo::StoreFactories;

fn create_store_factories() -> StoreFactories {
    StoreFactories::empty()
}

fn main() -> std::process::ExitCode {
    CliRunner::init()
        .add_store_factories(create_store_factories())
        .run()
        .into()
}
