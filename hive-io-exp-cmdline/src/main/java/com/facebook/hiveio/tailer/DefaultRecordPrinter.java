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
package com.facebook.hiveio.tailer;

import com.facebook.hiveio.record.HiveReadableRecord;

class DefaultRecordPrinter implements RecordPrinter {
  @Override public void printRecord(HiveReadableRecord record, int numColumns,
      Context context) {
    addRecordToStringBuilder(record, numColumns, context);
    context.perThread.get().flushBuffer();
  }

  public static void addRecordToStringBuilder(HiveReadableRecord record,
      int numColumns, Context context) {
    StringBuilder sb = context.perThread.get().stringBuilder;
    sb.append(String.valueOf(record.get(0)));
    for (int index = 1; index < numColumns; ++index) {
      sb.append(context.opts.separator);
      sb.append(String.valueOf(record.get(index)));
    }
  }
}
