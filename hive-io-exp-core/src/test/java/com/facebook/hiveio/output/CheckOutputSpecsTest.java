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

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;

import static org.testng.Assert.fail;

public class CheckOutputSpecsTest {
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
