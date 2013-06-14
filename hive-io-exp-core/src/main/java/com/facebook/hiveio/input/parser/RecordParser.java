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
package com.facebook.hiveio.input.parser;

import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.record.HiveReadableRecord;

import java.io.IOException;

/**
 * Interface for record parser
 *
 * @param <W> type being parsed
 */
public interface RecordParser<W extends Writable> {
  /**
   * Create an empty record. Will be called before any parse() calls are made.
   * Will be passed in to parse() so that parser can reuse the record.
   * @return record
   */
  HiveReadableRecord createRecord();

  /**
   * Parse a value.
   * @param value data to parse
   * @param record created from createRecord()
   * @return record with parsed data
   * @throws IOException if any problems occur
   */
  HiveReadableRecord parse(W value, HiveReadableRecord record)
    throws IOException;
}
