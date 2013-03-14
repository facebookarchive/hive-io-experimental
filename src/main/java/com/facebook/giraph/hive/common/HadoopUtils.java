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

package com.facebook.giraph.hive.common;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Helpers for dealing with Hadoop
 */
public class HadoopUtils {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HadoopUtils.class);

  /** Key in Configuration for output directory */
  private static final String OUTPUT_DIR_KEY = "mapred.output.dir";

  /** Don't construct, allow inheritance */
  protected HadoopUtils() { }

  /**
   * Set Configuration if object is Configurable
   * @param object Object to check
   * @param conf Configuration to set
   */
  public static void setConfIfPossible(Object object, Configuration conf) {
    if (object instanceof Configurable) {
      ((Configurable) object).setConf(conf);
    }
  }

  /**
   * Get Configuration if object is Configurable
   * @param object Object to check
   * @return Configuration if object is Configurable, else null
   */
  public static Configuration getConfIfPossible(Object object) {
    if (object instanceof Configurable) {
      return ((Configurable) object).getConf();
    }
    return null;
  }

  /**
   * Get output directory
   * @param conf Configuration to use
   * @return output directory
   */
  public static String getOutputDir(Configuration conf) {
    return conf.get(OUTPUT_DIR_KEY);
  }

  /**
   * Get path to output directory as Path
   * @param conf Configuration to use
   * @return Path to output directory
   */
  public static Path getOutputPath(Configuration conf) {
    return new Path(getOutputDir(conf));
  }

  /**
   * Set output directory to use
   * @param conf Configuration to use
   * @param path output directory
   */
  public static void setOutputDir(Configuration conf, String path) {
    conf.set(OUTPUT_DIR_KEY, path);
  }

  /**
   * Delete output directory for this job
   * @param conf Configuration to use
   * @throws IOException I/O errors
   */
  public static void deleteOutputDir(Configuration conf) throws IOException {
    FileSystem.get(conf).delete(getOutputPath(conf), true);
  }

  /**
   * Check if output committer needs success marker
   * @param conf Configuration to use
   * @return true if success marker required
   */
  public static boolean needSuccessMarker(Configuration conf) {
    return conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);
  }

  /**
   * Set Hadoop Output Key class
   * @param conf Configuration to use
   * @param writableClass Class that is Writable
   */
  public static void setOutputKeyWritableClass(Configuration conf,
      Class<? extends Writable> writableClass) {
    conf.set("mapred.output.key.class", writableClass.getName());
  }

  /**
   * Set Hadoop Output Value class
   * @param conf Configuration to use
   * @param writableClass Class that is Writable
   */
  public static void setOutputValueWritableClass(Configuration conf,
      Class<? extends Writable> writableClass) {
    conf.get("mapred.output.value.class", writableClass.getName());
  }

  /**
   * Set worker output directory
   * @param context Task context
   * @throws IOException I/O errors
   */
  public static void setWorkOutputDir(TaskAttemptContext context)
    throws IOException {
    Configuration conf = context.getConfiguration();
    String outputPath = getOutputDir(conf);
    // we need to do this to get the task path and set it for mapred
    // implementation since it can't be done automatically because of
    // mapreduce->mapred abstraction
    if (outputPath != null) {
      FileOutputCommitter foc =
          new FileOutputCommitter(getOutputPath(conf), context);
      conf.set("mapred.work.output.dir", foc.getWorkPath().toString());
    }
  }

  /**
   * Set MapReduce input directory
   *
   * @param conf Configuration to use
   * @param path path to set
   */
  public static void setInputDir(Configuration conf, String path) {
    conf.set("mapred.input.dir", path);
  }
}
