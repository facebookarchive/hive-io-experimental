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
package com.facebook.hiveio.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.HackJobContext;
import org.apache.hadoop.mapred.HackTaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.hiveio.common.HiveIOTestBase;
import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;

import static org.testng.Assert.fail;

public class CheckOutputSpecsTest extends HiveIOTestBase {
  private final String PROFILE_ID = "x";
  private final LocalHiveServer hiveServer = new LocalHiveServer("hiveio-test");

  @BeforeMethod
  public void beforeSuite() throws Exception {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test(expectedExceptions = IOException.class)
  public void testTableDoesntExist() throws Exception {
    Configuration conf = new Configuration();

    HiveOutputDescription outputDesc = new HiveOutputDescription();
    outputDesc.getTableDesc().setTableName("doesnt-exist");

    OutputConf outputConf = new OutputConf(conf, PROFILE_ID);
    outputConf.writeOutputDescription(outputDesc);

    HiveApiOutputFormat outputFormat = new HiveApiOutputFormat();
    outputFormat.setMyProfileId(PROFILE_ID);

    JobConf jobConf = new JobConf(conf);
    TaskAttemptContext taskContext =
        new HackTaskAttemptContext(jobConf, new TaskAttemptID());
    JobContext jobContext = new HackJobContext(jobConf, taskContext.getJobID());
    outputFormat.checkOutputSpecs(jobContext);
    fail();
  }
}
