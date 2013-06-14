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

import org.apache.hadoop.hive.ql.stats.StatsSetupConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.barney4j.utils.unit.ByteUnit;
import com.google.common.base.Objects;

import java.util.Map;

import static java.lang.Long.parseLong;

/**
 * Information about a table or partition. See "DESC FORMATTED table"
 */
public class HiveStats {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(HiveStats.class);

  /** total size */
  private long totalSize;
  /** raw size */
  private long rawSize;
  /** number of rows in table */
  private long numRows;

  /** Constructor */
  public HiveStats() {
    totalSize = 0;
    rawSize = 0;
    numRows = 0;
  }

  /**
   * Constructor
   *
   * @param numRows number of rows
   * @param rawSize raw size
   * @param totalSize total size
   */
  public HiveStats(long numRows, long rawSize, long totalSize) {
    this.numRows = numRows;
    this.rawSize = rawSize;
    this.totalSize = totalSize;
  }

  public long getNumRows() {
    return numRows;
  }

  public long getRawSizeInBytes() {
    return rawSize;
  }

  public double getRawSizeInMB() {
    return ByteUnit.BYTE.toMB(rawSize);
  }

  public long getTotalSizeInBytes() {
    return totalSize;
  }

  public double getTotalSizeInMB() {
    return ByteUnit.BYTE.toMB(totalSize);
  }

  /**
   * Add another HiveStats
   *
   * @param other HiveStats to add
   */
  public void add(HiveStats other) {
    totalSize += other.totalSize;
    rawSize += other.rawSize;
    numRows += other.numRows;
  }

  /**
   * Parse HiveStats from table parameters retrieved from thrift.
   *
   * @param params thrift table params
   * @return HiveStats
   */
  public static HiveStats fromParams(Map<String, String> params) {
    LOG.info("Table params: {}", params);
    long totalSize = parseLong(params.get(StatsSetupConst.TOTAL_SIZE));
    long rawSize = parseLong(params.get(StatsSetupConst.RAW_DATA_SIZE));
    long numRows = parseLong(params.get(StatsSetupConst.ROW_COUNT));
    return new HiveStats(numRows, rawSize, totalSize);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("totalSize", totalSize)
        .add("rawSize", rawSize)
        .add("numRows", numRows)
        .toString();
  }
}
