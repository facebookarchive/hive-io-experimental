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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;

import com.facebook.hiveio.output.HiveOutputDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetastoreDesc implements Writable {
  /** Hive Metastore Host. If not set will infer from HiveConf */
  private String host;
  /** Hive Metastore Port */
  private int port = 9083;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public ThriftHiveMetastore.Iface makeClient(Configuration conf)
      throws TException {
    ThriftHiveMetastore.Iface client;
    if (host != null) {
      client = HiveMetastores.create(host, port);
    } else {
      HiveConf hiveConf = new HiveConf(conf, HiveOutputDescription.class);
      client = HiveMetastores.create(hiveConf);
    }
    return client;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    host = WritableUtils.readString(in);
    port = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, host);
    out.writeInt(port);
  }
}
