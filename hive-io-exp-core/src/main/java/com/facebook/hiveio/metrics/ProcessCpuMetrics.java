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
 * Metrics for CPUs
 */
public class ProcessCpuMetrics {
  /** Don't construct */
  private ProcessCpuMetrics() { }

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
    registerMetrics(metrics, ProcessCpuMetrics.class);
  }

  /**
   * Registery metrics tracked here
   *
   * @param metrics MetricsRegistry
   * @param owningClass class owning metrics
   */
  public static void registerMetrics(MetricsRegistry metrics, Class<?> owningClass)
  {
    final Sigar sigar = new Sigar();
    final long pid = sigar.getPid();
    metrics.newGauge(owningClass, "process-cpu-user", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcCpu(pid).getUser();
      }
    });
    metrics.newGauge(owningClass, "process-cpu-system", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcCpu(pid).getSys();
      }
    });
    metrics.newGauge(owningClass, "process-cpu-system", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcCpu(pid).getStartTime();
      }
    });
    metrics.newGauge(owningClass, "process-cpu-total", new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getProcCpu(pid).getTotal();
      }
    });
    metrics.newGauge(owningClass, "process-cpu-pct", new SigarDoubleGauge() {
      @Override public double computeValue() throws SigarException {
        return sigar.getProcCpu(pid).getPercent();
      }
    });
  }
}
