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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.conf.HiveHooks;
import com.facebook.hiveio.conf.IntConfOption;
import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Wrapper around Thrift MetasStore client with helper methods.
 */
public class HiveMetastores {
  /** Option for metastore timeout */
  public static final IntConfOption METASTORE_TIMEOUT_MS =
      new IntConfOption("hiveio.metastore.timeout.ms", (int) SECONDS.toMillis(60));

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(
      HiveMetastores.class);

  /**
   * This is mainly for testing, it should not be used normally. It allows us to
   * have a global metastore connection that overrides everything else.
   */
  private static ThriftHiveMetastore.Iface TEST_CLIENT;

  /** Don't construct */
  private HiveMetastores() { }

  @VisibleForTesting
  public static ThriftHiveMetastore.Iface getTestClient() {
    return TEST_CLIENT;
  }

  @VisibleForTesting
  public static void setTestClient(ThriftHiveMetastore.Iface testClient) {
    HiveMetastores.TEST_CLIENT = testClient;
  }

  /**
   * Create client from host and port with default timeout
   * @param host Host to connect to
   * @param port Port to connect to
   * @return Thrift Hive interface connected to host:port
   * @throws TTransportException network problems
   */
  public static ThriftHiveMetastore.Iface create(String host, int port)
    throws TTransportException
  {
    if (TEST_CLIENT != null) {
      return TEST_CLIENT;
    }
    return create(host, port, METASTORE_TIMEOUT_MS.getDefaultValue());
  }

  /**
   * Create client from a HiveConf.
   *
   * First we try to read the URIs out of the HiveConf and connect to those. If
   * that fails we create Hive's client and grab the thrift connection out of
   * that using reflection.
   *
   * @param hiveConf HiveConf to use
   * @return Thrift Hive client
   * @throws TException network problems
   */
  public static ThriftHiveMetastore.Iface create(HiveConf hiveConf)
    throws TException
  {
    if (TEST_CLIENT != null) {
      return TEST_CLIENT;
    }
    ThriftHiveMetastore.Iface client = createFromURIs(hiveConf);
    if (client == null) {
      client = createfromReflection(hiveConf);
    }
    return client;
  }

  /**
   * Create client from host and port with timeout
   * @param host Host to connect to
   * @param port Port to connect on
   * @param timeoutMillis Socket timeout
   * @return Hive Thrift Metastore client
   * @throws TTransportException Connection errors
   */
  public static ThriftHiveMetastore.Iface create(String host, int port,
    int timeoutMillis) throws TTransportException
  {
    if (TEST_CLIENT != null) {
      return TEST_CLIENT;
    }
    TTransport transport = new TSocket(host, port, timeoutMillis);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new ThriftHiveMetastore.Client(protocol);
  }

  /**
   * Create client by instantiating Hive's own client and using reflection to
   * grab the thrift client out of that.
   *
   * @param hiveConf HiveConf to use
   * @return Thrift Hive client
   * @throws TException network problems
   */
  private static ThriftHiveMetastore.Iface createfromReflection(
    HiveConf hiveConf) throws TException  {
    HiveMetaStoreClient hiveClient;
    try {
      hiveClient = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      throw new TException(e);
    }

    Field clientField;
    try {
      clientField = hiveClient.getClass().getDeclaredField("client");
    } catch (NoSuchFieldException e) {
      throw new TException(e);
    }
    clientField.setAccessible(true);

    ThriftHiveMetastore.Iface thriftIface;
    try {
      thriftIface = (ThriftHiveMetastore.Iface) clientField.get(hiveClient);
    } catch (IllegalAccessException e) {
      throw new TException(e);
    }

    return thriftIface;
  }

  /**
   * Create Hive client from URIs in HiveConf
   *
   * @param conf HiveConf to use
   * @return Thrift Hive client, or null if could not make one out of URIs
   */
  private static ThriftHiveMetastore.Iface createFromURIs(HiveConf conf) {
    List<URI> uris = HiveUtils.getURIs(conf, HiveConf.ConfVars.METASTOREURIS);
    if (uris.isEmpty()) {
      // Sometimes the Hive hooks are the ones responsible for filling in
      // the metastore URIs, so try to run them here.
      HiveHooks.runDriverPreHooks(conf);
      uris = HiveUtils.getURIs(conf, HiveConf.ConfVars.METASTOREURIS);
      if (uris.isEmpty()) {
        LOG.error("No Hive Metastore URIs to connect to after running hooks");
        return null;
      }
    }

    Collections.shuffle(uris);

    for (URI uri : uris) {
      try {
        LOG.info("Connecting to metastore at " +
            uri.getHost() + ":" + uri.getPort());
        return create(uri.getHost(), uri.getPort());
      } catch (TTransportException e) {
        LOG.warn("Failed to connect to {}:{}", uri.getHost(), uri.getPort(), e);
      }
    }
    return null;
  }
}
