package com.facebook.giraph.hive.common;

import com.google.common.base.Objects;

public class HiveTableName {
  private final String databaseName;
  private final String tableName;

  public HiveTableName(String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String dotString() {
    return databaseName + "." + tableName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("databaseName", databaseName)
        .add("tableName", tableName)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveTableName that = (HiveTableName) o;

    return Objects.equal(databaseName, that.databaseName) &&
        Objects.equal(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, tableName);
  }
}
