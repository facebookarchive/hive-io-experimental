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
package com.facebook.hiveio.cmdline;

public class Defaults {
  private static final String PLATINUM_METASTORE_HOST = "";
  private static final String SILVER_METASTORE_HOST = "hadoopminimstr032.frc1.facebook.com";

  public static final String METASTORE_HOST = SILVER_METASTORE_HOST;
  public static final int METASTORE_PORT = 9083;

  public static final String DATABASE = "default";

  public static final String PARTITION_FILTER = "";

  public static final String SEPARATOR = "\t";

  private Defaults() {}
}
