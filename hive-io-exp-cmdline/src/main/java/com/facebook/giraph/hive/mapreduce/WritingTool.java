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
package com.facebook.giraph.hive.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.facebook.giraph.hive.common.HadoopUtils;
import com.facebook.giraph.hive.record.HiveWritableRecord;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/*
  CREATE TABLE hive_io_test (
    i1 BIGINT,
    i2 BIGINT,
    i3 BIGINT
  )
  PARTITIONED BY (ds STRING)
  TBLPROPERTIES ('RETENTION_PLATINUM'='90')
 */
public class WritingTool extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WritingTool.class);

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    // TODO: make configurable through cmdline
    HadoopUtils.setPool(conf, "di.nonsla");
    HadoopUtils.setMapAttempts(conf, 1);
    adjustConfigurationForHive(conf);

    Job job = new Job(conf, "hive-io-writing");
    if (job.getJar() == null) {
      job.setJarByClass(getClass());
    }
    job.setMapperClass(SampleMapper.class);
    job.setInputFormatClass(SampleInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(HiveWritableRecord.class);
    job.setOutputFormatClass(SampleOutputFormat.class);

    job.setNumReduceTasks(0);

    job.submit();
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
  * set hive configuration
  */
  private void adjustConfigurationForHive(Configuration conf) {
    // when output partitions are used, workers register them to the
    // metastore at cleanup stage, and on HiveConf's initialization, it
    // looks for hive-site.xml.
    addToStringCollection(conf, "tmpfiles",
      conf.getClassLoader().getResource("hive-site.xml").toString());

    // Or, more effectively, we can provide all the jars client needed to
    // the workers as well
    String[] hadoopJars = System.getenv("HADOOP_CLASSPATH").split(
        File.pathSeparator);
    List<String> hadoopJarURLs = Lists.newArrayList();
    for (String jarPath : hadoopJars) {
      File file = new File(jarPath);
      if (file.exists() && file.isFile()) {
        String jarURL = file.toURI().toString();
        hadoopJarURLs.add(jarURL);
      }
    }
    addToStringCollection(conf, "tmpjars", hadoopJarURLs);
  }

  /**
  * add string to collection
  * @param conf Configuration
  * @param name name to add
  * @param values values for collection
  */
  private static void addToStringCollection(Configuration conf, String name,
                                            String... values) {
    addToStringCollection(conf, name, Arrays.asList(values));
  }

  /**
  * add string to collection
  * @param conf Configuration
  * @param name to add
  * @param values values for collection
  */
  private static void addToStringCollection(
          Configuration conf, String name, Collection<? extends String> values) {
    Collection<String> tmpfiles = conf.getStringCollection(name);
    tmpfiles.addAll(values);
    conf.setStrings(name, tmpfiles.toArray(new String[tmpfiles.size()]));
  }
}
