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
package com.facebook.hiveio.options;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.transport.TTransportException;

import com.facebook.hiveio.common.HiveMetastores;
import io.airlift.command.Option;

/**
 * Options for Metastore
 */
public class MetastoreOptions {
  // CHECKSTYLE: stop VisibilityModifier
  /** Hive host */
  @Option(name = { "--metastore-host" }, description = "Hive Metastore Host")
  public String host = Defaults.METASTORE_HOST;

  /** Hive port */
  @Option(name = { "--metastore-port" }, description = "Hive Metatstore Port")
  public int port = Defaults.METASTORE_PORT;
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Create Metastore Thrift client
   *
   * @return thrift metastore client
   * @throws TTransportException
   */
  public ThriftHiveMetastore.Iface makeClient() throws TTransportException {
    return HiveMetastores.create(host, port);
  }
}
