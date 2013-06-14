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

import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.List;

/**
 * Metrics for network related things
 */
public class NetworkMetrics {
  /** Don't construct */
  private NetworkMetrics() { }

  /**
   * Register metrics tracked here
   *
   * @throws SigarException
   */
  public static void registerMetrics() throws SigarException {
    registerMetrics(Metrics.defaultRegistry());
  }

  /**
   * Register metrics tracked here
   *
   * @param metrics MetricsRegistry
   * @throws SigarException
   */
  public static void registerMetrics(MetricsRegistry metrics) throws SigarException {
    registerMetrics(metrics, NetworkMetrics.class);
  }

  /**
   * Register metrics tracked here
   *
   * @param interfaceNames Names of network interfaces
   * @throws SigarException
   */
  public static void registerMetrics(List<String> interfaceNames)
    throws SigarException
  {
    if (interfaceNames.isEmpty()) {
      registerMetrics(Metrics.defaultRegistry(), NetworkMetrics.class);
    } else {
      final Sigar sigar = new Sigar();
      for (String interfaceName : interfaceNames) {
        registerMetrics(Metrics.defaultRegistry(), NetworkMetrics.class, interfaceName, sigar);
      }
    }
  }

  /**
   * Register metrics tracked here
   *
   * @param metrics MetricsRegistry
   * @param owningClass class that should own metrics
   * @throws SigarException
   */
  public static void registerMetrics(MetricsRegistry metrics, Class<?> owningClass)
    throws SigarException
  {
    final Sigar sigar = new Sigar();
    String[] interfaceNames = sigar.getNetInterfaceList();
    for (String interfaceName : interfaceNames) {
      registerMetrics(metrics, owningClass, interfaceName, sigar);
    }
  }

  /**
   * Register metrics tracked here
   *
   * @param metrics MetricsRegistry
   * @param owningClass class that should own metrics
   * @param interfaceName network interface names
   * @param sigar Hyperic Sigar object
   * @throws SigarException
   */
  public static void registerMetrics(MetricsRegistry metrics, Class<?> owningClass,
    final String interfaceName, final Sigar sigar) throws SigarException
  {
    NetInterfaceStat netInterfaceStat = sigar.getNetInterfaceStat(interfaceName);
    final long initialRxBytes = netInterfaceStat.getRxBytes();
    final long initialRxPackets = netInterfaceStat.getRxPackets();
    final long initialTxBytes = netInterfaceStat.getTxBytes();
    final long initialTxPackets = netInterfaceStat.getTxPackets();

    // CHECKSTYLE: stop IndentationCheck
    metrics.newGauge(owningClass, "network-" + interfaceName + "-receive-bytes",
        new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getNetInterfaceStat(interfaceName).getRxBytes() - initialRxBytes;
      }
    });
    metrics.newGauge(owningClass, "network-" + interfaceName + "-receive-packets",
        new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getNetInterfaceStat(interfaceName).getRxPackets() - initialRxPackets;
      }
    });
    metrics.newGauge(owningClass, "network-" + interfaceName + "-send-bytes",
        new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getNetInterfaceStat(interfaceName).getTxBytes() - initialTxBytes;
      }
    });
    metrics.newGauge(owningClass, "network-" + interfaceName + "-send-packets",
        new SigarLongGauge() {
      @Override public long computeValue() throws SigarException {
        return sigar.getNetInterfaceStat(interfaceName).getTxPackets() - initialTxPackets;
      }
    });
    // CHECKSTYLE: resume IndentationCheck
  }
}
