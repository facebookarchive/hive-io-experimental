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
package com.facebook.hiveio.testing;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.TException;

import com.facebook.hiveio.common.HiveUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

public class LocalHiveServer {
  public static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

  private final String name;
  private File baseDir;
  private HiveConf hiveConf;
  private HiveServer.HiveServerHandler client;

  public LocalHiveServer(String name) {
    this.name = name;
  }

  public void init() throws IOException, TException
  {
    if (baseDir != null && baseDir.isDirectory()) {
      FileUtils.deleteDirectory(baseDir);
    }

    hiveConf = HiveUtils.newHiveConf();
    baseDir = randomDir();

    File metastore = new File(baseDir, "metastore");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREDIRECTORY, metastore.getAbsolutePath());

    File warehouse = new File(baseDir, "warehouse");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouse.getAbsolutePath());

    File metastoreDb = new File(baseDir, "metastore_db");
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        "jdbc:derby:;databaseName=" + metastoreDb.getAbsolutePath() + ";create=true");

    client = new HiveServer.HiveServerHandler(hiveConf);
    client.execute("CREATE DATABASE IF NOT EXISTS default");
  }

  public void dropTable(String name) throws TException {
    client.execute("DROP TABLE " + name);
  }

  public void createTable(String hql) throws TException {
    client.execute(hql);
  }

  public void loadData(String tableName, String[] data)
      throws IOException, TException
  {
    File dataFile = writeDataToFile(tableName, data);
    client.execute("LOAD DATA LOCAL INPATH '" + dataFile.getAbsolutePath() +
        "' OVERWRITE INTO TABLE " + tableName);
  }

  public void loadData(String tableName, String partitionStr, String[] data)
      throws IOException, TException
  {
    File dataFile = writeDataToFile(tableName, data);
    client.execute("LOAD DATA LOCAL INPATH '" + dataFile.getAbsolutePath() +
        "' OVERWRITE INTO TABLE " + tableName +
        " PARTITION (" + partitionStr + ")");
  }

  private File writeDataToFile(String tableName, String[] data)
      throws IOException {
    File dataFile = new File(rootDir(), tableName);
    Writer writer = Files.newWriter(dataFile, Charsets.UTF_8);
    for (String line : data) {
      writer.write(line);
      writer.write("\n");
    }
    writer.close();
    return dataFile;
  }

  public File rootDir() {
    return TMP_DIR;
  }

  public File randomDir() {
    return new File(rootDir(), name + "-" + System.currentTimeMillis());
  }

  public HiveInterface getClient() {
    return client;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public String getName() {
    return name;
  }
}
