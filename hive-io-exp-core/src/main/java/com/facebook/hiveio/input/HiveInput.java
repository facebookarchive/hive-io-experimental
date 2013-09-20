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
package com.facebook.hiveio.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.bean.RowToBean;
import com.facebook.hiveio.bean.UnsafeRowToBean;
import com.facebook.hiveio.common.HiveUtils;
import com.facebook.hiveio.common.RecordReaderWrapper;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Simple API for reading from Hive
 */
public class HiveInput {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveInput.class);

  /** Don't construct */
  private HiveInput() { }

  /**
   * Read a Hive table
   *
   * @param inputDesc Hive table description
   * @return Iterable of Hive records
   * @throws IOException
   * @throws InterruptedException
   */
  public static Iterable<HiveReadableRecord> readTable(HiveInputDescription inputDesc)
    throws IOException, InterruptedException
  {
    final HiveConf conf = HiveUtils.newHiveConf(HiveInput.class);
    return readTable(inputDesc, conf);
  }

  /**
   * Read a Hive table
   *
   * @param inputDesc Hive table description
   * @param conf Configuration
   * @return Iterable of Hive records
   * @throws IOException
   * @throws InterruptedException
   */
  public static Iterable<HiveReadableRecord> readTable(HiveInputDescription inputDesc,
      final Configuration conf) throws IOException, InterruptedException
  {
    String profileID = Long.toString(System.currentTimeMillis());

    HiveApiInputFormat.setProfileInputDesc(conf, inputDesc, profileID);

    final HiveApiInputFormat inputFormat = new HiveApiInputFormat();
    inputFormat.setMyProfileId(profileID);

    JobContext jobContext = new JobContext(conf, new JobID());
    final List<InputSplit> splits = inputFormat.getSplits(jobContext);

    return new Iterable<HiveReadableRecord>() {
      @Override public Iterator<HiveReadableRecord> iterator() {
        return new RecordIterator(inputFormat, conf, splits.iterator());
      }
    };
  }

  /**
   * Create a row-to-java-bean transformer
   *
   * @param inputDesc Hive input description
   * @param rowClass Class of Java bean
   * @param <X> type of Java bean
   * @return RowToBean
   */
  public static <X> RowToBean<X> rowToBean(HiveInputDescription inputDesc, Class<X> rowClass) {
    try {
      HiveConf conf = HiveUtils.newHiveConf(HiveInput.class);
      HiveTableSchema schema = HiveTableSchemas.lookup(conf, inputDesc.getTableDesc());
      RowToBean<X> rowToBean = new UnsafeRowToBean<X>(rowClass, schema);
      return rowToBean;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Read table into iterable of user Java bean objects
   *
   * @param inputDesc Hive input description
   * @param rowClass Class of Java bean
   * @param <X> type of Java bean
   * @return Iterable over user objects
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws IOException
   * @throws InterruptedException
   */
  public static <X> Iterable<X> readTable(HiveInputDescription inputDesc, Class<X> rowClass)
    throws IllegalAccessException, InstantiationException, IOException, InterruptedException
  {
    final X row = rowClass.newInstance();
    final RowToBean<X> rowMapper = rowToBean(inputDesc, rowClass);
    Function<HiveReadableRecord, X> function = new Function<HiveReadableRecord, X>() {
      @Nullable @Override public X apply(@Nullable HiveReadableRecord input) {
        rowMapper.writeRow(input, row);
        return row;
      }
    };
    return Iterables.transform(readTable(inputDesc), function);
  }

  /**
   * Iterator over records
   */
  private static class RecordIterator extends AbstractIterator<HiveReadableRecord> {
    /** input format */
    private final HiveApiInputFormat inputFormat;
    /** input splits */
    private final Iterator<InputSplit> splits;
    /** context */
    private final TaskAttemptContext taskContext;
    /** record reader */
    private RecordReaderWrapper<HiveReadableRecord> recordReader;

    /**
     * Constructor
     *
     * @param inputFormat Hive table InputFormat
     * @param conf Configuration
     * @param splits input splits
     */
    public RecordIterator(HiveApiInputFormat inputFormat, Configuration conf,
      Iterator<InputSplit> splits)
    {
      this.inputFormat = inputFormat;
      this.splits = splits;
      this.taskContext = new TaskAttemptContext(conf, new TaskAttemptID());
    }

    @Override
    protected HiveReadableRecord computeNext() {
      while (recordReader == null || !recordReader.hasNext()) {
        RecordReader<WritableComparable, HiveReadableRecord> reader = null;
        while (splits.hasNext() && reader == null) {
          InputSplit split = splits.next();
          try {
            reader = inputFormat.createRecordReader(split, taskContext);
            reader.initialize(split, taskContext);
            // CHECKSTYLE: stop IllegalCatch
          } catch (Exception e) {
            // CHECKSTYLE: stop IllegalCatch
            LOG.info("Couldn't create reader from split {}", split, e);
          }
        }
        if (reader == null) {
          return endOfData();
        } else {
          recordReader = new RecordReaderWrapper<HiveReadableRecord>(reader);
        }
      }

      return recordReader.next();
    }
  }
}
