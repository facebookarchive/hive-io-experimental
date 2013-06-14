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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.hiveio.common.HostPort;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.airlift.command.Help;
import io.airlift.command.Option;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Options for Hive namespaces
 */
public class NamespaceOptions {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceOptions.class);

  // CHECKSTYLE: stop VisibilityModifier
  /** Path to file with namespaces */
  @Option(name = "--namespaces-file", description = "File of Hive metastore clusters")
  public String path;

  /** Namespace to use */
  @Option(name = "--namespace", description = "Cluster to use")
  public String name = "silver";
  // CHECKSTYLE: resume VisibilityModifier

  /**
   * Check if file path is present
   *
   * @return true if path present
   */
  public boolean hasPath() {
    return path != null;
  }

  /**
   * Read metastore information from file path
   *
   * @return HostPort of metastore
   * @throws IOException
   */
  public HostPort readMetastoreInfo() throws IOException {
    if (path == null) {
      LOG.error("Cluster file not given");
      new Help().run();
      return null;
    }
    ObjectMapper objectMapper = new ObjectMapper();
    File file = new File(path);
    NamespaceData clustersData = objectMapper.readValue(file, NamespaceData.class);
    List<HostPort> hostAndPorts = clustersData.data.get(name);
    if (hostAndPorts == null) {
      LOG.error("Cluster {} not found in data file {}", name, path);
      return null;
    }
    Collections.shuffle(hostAndPorts);
    return hostAndPorts.get(0);
  }

  /**
   * Holder for namespace information
   */
  private static class NamespaceData {
    // CHECKSTYLE: stop VisibilityModifier
    /** Raw data */
    @JsonProperty public Map<String, List<HostPort>> data = Maps.newHashMap();
    // CHECKSTYLE: resume VisibilityModifier
  }
}
