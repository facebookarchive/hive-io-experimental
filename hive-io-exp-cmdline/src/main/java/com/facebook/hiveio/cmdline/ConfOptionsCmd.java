/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hiveio.cmdline;

import com.facebook.hiveio.conf.AllOptions;
import com.facebook.hiveio.options.BaseCmd;
import io.airlift.command.Command;

/**
 * Command that print out configuration options
 */
@Command(name = "conf-options", description = "Print Configuration Options")
public class ConfOptionsCmd extends BaseCmd {
  @Override public void execute() throws Exception {
    AllOptions.main(new String[0]);
  }
}
