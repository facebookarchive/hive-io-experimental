package com.facebook.hiveio.benchmark;


import com.sampullara.cli.Argument;
import io.airlift.command.Option;

public class InputBenchmarkCmd implements Runnable {
  /** Hive host */
  @Option(name = {"-h", "--host"}, description = "Hive Metastore host")
  private String hiveHost = "hadoopminimstr032.frc1.facebook.com";

  /** Hive port */
  @Option(name = {"-p", "--port"}, description = "Hive Metatstore port")
  private int hivePort = 9083;

  /** Hive database */
  @Option(name = "--database", description = "Hive database to use")
  private String database = "default";

  /** Hive table */
  @Option(name = {"-t", "--table"}, description = "Hive table to query")
  private String table = "inference_sims";

  /** Partition filter */
  @Argument private String partitionFilter = "ds='2013-01-01' and feature='test_nz'";

  /** Whether to track metrics */
  @Argument private boolean trackMetrics = false;

  /** Every how many splits to print */
  @Argument private int splitPrintPeriod = 3;

  /** Every how many records in a split to print */
  @Argument private int recordPrintPeriod = 1000000;

  /** Print usage */
  @Argument private boolean help = false;

  @Override public void run() {
  }
}
