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
package com.facebook.hiveio.common;

import com.facebook.hiveio.options.Defaults;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A host-port pair
 */
public class HostPort {
  // CHECKSTYLE: stop VisibilityModifier
  /** host */
  @JsonProperty public String host;
  /** port */
  @JsonProperty public int port = Defaults.METASTORE_PORT;
  // CHECKSTYLE: resume VisibilityModifier

  /** Empty constructor */
  public HostPort() { }

  /**
   * Constructor
   *
   * @param host the host
   * @param port the port
   */
  public HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override public String toString() {
    return host + ":" + port;
  }
}
