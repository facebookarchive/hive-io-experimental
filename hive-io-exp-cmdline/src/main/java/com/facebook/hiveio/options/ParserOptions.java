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
package com.facebook.hiveio.options;

import com.facebook.hiveio.rows.IdIdSimRow;
import io.airlift.command.Option;

/**
 * Options for parser
 */
public class ParserOptions {
  // CHECKSTYLE: stop VisibilityModifier
  /** Parse only, don't print */
  @Option(name = { "--parse-only", "--dont-print" },
      description = "Don't print, just measure performance")
  public boolean parseOnly = false;

  /** Name of Java bean parser */
  @Option(name = "--bean-parser", description = "Use bean parser")
  public boolean beanParser = false;

  /** Name of row class */
  @Option(name = "--row-class", description = "class which represents a row")
  public String rowClassName = IdIdSimRow.class.getName();
  // CHECKSTYLE: stop VisibilityModifier
}
