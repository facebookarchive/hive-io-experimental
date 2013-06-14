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
package com.facebook.hiveio.metrics;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Memory metrics
 */
public class ProcessMemoryMetrics {
  /** Don't construct */
  private ProcessMemoryMetrics() { }

  /** Register metrics tracked here */
  public static void registerMetrics() {
    registerMetrics(Metrics.defaultRegistry());
  }

  /**
   * Register metrics tracked here
   *
   * @param metrics MetricsRegistry
   */
  public static void registerMetrics(MetricsRegistry metrics) {
    registerMetrics(metrics, ProcessMemoryMetrics.class);
  }

  /**
   * Register metrics tracked here
   *
   * @param metrics MetricsRegistry
   * @param owningClass class owning metrics
   */
  public static void registerMetrics(MetricsRegistry metrics, Class<?> owningClass) {
    final Sigar sigar = new Sigar();
    final long pid = sigar.getPid();
    metrics.newGauge(owningClass, "process-memory-virtual", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcMem(pid).getSize();
      }
    });
    metrics.newGauge(owningClass, "process-memory-resident", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcMem(pid).getResident();
      }
    });
    metrics.newGauge(owningClass, "process-memory-shared", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcMem(pid).getShare();
      }
    });
    metrics.newGauge(owningClass, "process-memory-page-faults", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcMem(pid).getPageFaults();
      }
    });
  }
}
