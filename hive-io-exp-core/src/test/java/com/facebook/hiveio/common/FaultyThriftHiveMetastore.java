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

package com.facebook.hiveio.common;

import com.facebook.fb303.fb_status;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * A dummy implementation that throws TException on selected methods.
 */
public class FaultyThriftHiveMetastore implements ThriftHiveMetastore.Iface {
  private final int numFailures;
  private int numCalls;

  public FaultyThriftHiveMetastore(int numFaliures) {
    this.numFailures = numFaliures;
    this.numCalls = 0;
  }

  public int getNumCalls() {
    return numCalls;
  }

  @Override
  public void create_database(Database database) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
  }

  @Override
  public Database get_database(String s) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public void drop_database(String s, boolean b, boolean b2) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  }

  @Override
  public List<String> get_databases(String s) throws MetaException, TException {
    return null;
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    return null;
  }

  @Override
  public void alter_database(String s, Database database) throws MetaException, NoSuchObjectException, TException {
  }

  @Override
  public Type get_type(String s) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public boolean create_type(Type type) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_type(String s) throws MetaException, NoSuchObjectException, TException {
    return false;
  }

  @Override
  public Map<String, Type> get_type_all(String s) throws MetaException, TException {
    return null;
  }

  @Override
  public List<FieldSchema> get_fields(String s, String s2) throws MetaException, UnknownTableException, UnknownDBException, TException {
    return null;
  }

  @Override
  public List<FieldSchema> get_schema(String s, String s2) throws MetaException, UnknownTableException, UnknownDBException, TException {
    return null;
  }

  @Override
  public void create_table(Table table) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
  }

  @Override
  public void create_table_with_environment_context(Table table, EnvironmentContext environmentContext) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
  }

  @Override
  public void drop_table(String s, String s2, boolean b) throws NoSuchObjectException, MetaException, TException {
  }

  @Override
  public void drop_table_with_environment_context(String s, String s2, boolean b, EnvironmentContext environmentContext) throws NoSuchObjectException, MetaException, TException {
  }

  @Override
  public List<String> get_tables(String s, String s2) throws MetaException, TException {
    return null;
  }

  @Override
  public List<String> get_all_tables(String s) throws MetaException, TException {
    return null;
  }

  @Override
  public Table get_table(String s, String s2) throws MetaException, NoSuchObjectException, TException {
    numCalls++;
    if (numCalls <= numFailures) {
      throw new TException();
    } else {
      return null;
    }
  }

  @Override
  public List<Table> get_table_objects_by_name(String s, List<String> strings) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return null;
  }

  @Override
  public List<String> get_table_names_by_filter(String s, String s2, short i) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return null;
  }

  @Override
  public void alter_table(String s, String s2, Table table) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public void alter_table_with_environment_context(String s, String s2, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public Partition add_partition(Partition partition) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public Partition add_partition_with_environment_context(Partition partition, EnvironmentContext environmentContext) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public int add_partitions(List<Partition> partitions) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return 0;
  }

  @Override
  public Partition append_partition(String s, String s2, List<String> strings) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public Partition append_partition_with_environment_context(String s, String s2, List<String> strings, EnvironmentContext environmentContext) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public Partition append_partition_by_name(String s, String s2, String s3) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(String s, String s2, String s3, EnvironmentContext environmentContext) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public boolean drop_partition(String s, String s2, List<String> strings, boolean b) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_partition_with_environment_context(String s, String s2, List<String> strings, boolean b, EnvironmentContext environmentContext) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_partition_by_name(String s, String s2, String s3, boolean b) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(String s, String s2, String s3, boolean b, EnvironmentContext environmentContext) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public Partition get_partition(String s, String s2, List<String> strings) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public Partition get_partition_with_auth(String s, String s2, List<String> strings, String s3, List<String> strings2) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public Partition get_partition_by_name(String s, String s2, String s3) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions(String s, String s2, short i) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions_with_auth(String s, String s2, short i, String s3, List<String> strings) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<String> get_partition_names(String s, String s2, short i) throws MetaException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions_ps(String s, String s2, List<String> strings, short i) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(String s, String s2, List<String> strings, short i, String s3, List<String> strings2) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<String> get_partition_names_ps(String s, String s2, List<String> strings, short i) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions_by_filter(String s, String s2, String s3, short i) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Partition> get_partitions_by_names(String s, String s2, List<String> strings) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public void alter_partition(String s, String s2, Partition partition) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public void alter_partitions(String s, String s2, List<Partition> partitions) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public void alter_partition_with_environment_context(String s, String s2, Partition partition, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public void rename_partition(String s, String s2, List<String> strings, Partition partition) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public boolean partition_name_has_valid_characters(List<String> strings, boolean b) throws MetaException, TException {
    return false;
  }

  @Override
  public String get_config_value(String s, String s2) throws ConfigValSecurityException, TException {
    return null;
  }

  @Override
  public List<String> partition_name_to_vals(String s) throws MetaException, TException {
    return null;
  }

  @Override
  public Map<String, String> partition_name_to_spec(String s) throws MetaException, TException {
    return null;
  }

  @Override
  public void markPartitionForEvent(String s, String s2, Map<String, String> stringStringMap, PartitionEventType partitionEventType) throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException, InvalidPartitionException, TException {
  }

  @Override
  public boolean isPartitionMarkedForEvent(String s, String s2, Map<String, String> stringStringMap, PartitionEventType partitionEventType) throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException, InvalidPartitionException, TException {
    return false;
  }

  @Override
  public Index add_index(Index index, Table table) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public void alter_index(String s, String s2, String s3, Index index) throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public boolean drop_index_by_name(String s, String s2, String s3, boolean b) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public Index get_index_by_name(String s, String s2, String s3) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Index> get_indexes(String s, String s2, short i) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<String> get_index_names(String s, String s2, short i) throws MetaException, TException {
    return null;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics columnStatistics) throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return false;
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics columnStatistics) throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return false;
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String s, String s2, String s3) throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    return null;
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String s, String s2, String s3, String s4) throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    return null;
  }

  @Override
  public boolean delete_partition_column_statistics(String s, String s2, String s3, String s4) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return false;
  }

  @Override
  public boolean delete_table_column_statistics(String s, String s2, String s3) throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return false;
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_role(String s) throws MetaException, TException {
    return false;
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return null;
  }

  @Override
  public boolean grant_role(String s, String s2, PrincipalType principalType, String s3, PrincipalType principalType2, boolean b) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean revoke_role(String s, String s2, PrincipalType principalType) throws MetaException, TException {
    return false;
  }

  @Override
  public List<Role> list_roles(String s, PrincipalType principalType) throws MetaException, TException {
    return null;
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s, List<String> strings) throws MetaException, TException {
    return null;
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType, HiveObjectRef hiveObjectRef) throws MetaException, TException {
    return null;
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
    return false;
  }

  @Override
  public List<String> set_ugi(String s, List<String> strings) throws MetaException, TException {
    return null;
  }

  @Override
  public String get_delegation_token(String s, String s2) throws MetaException, TException {
    return null;
  }

  @Override
  public long renew_delegation_token(String s) throws MetaException, TException {
    return 0;
  }

  @Override
  public void cancel_delegation_token(String s) throws MetaException, TException {
  }

  @Override
  public String getName() throws TException {
    return null;
  }

  @Override
  public String getVersion() throws TException {
    return null;
  }

  @Override
  public fb_status getStatus() throws TException {
    return null;
  }

  @Override
  public String getStatusDetails() throws TException {
    return null;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    return null;
  }

  @Override
  public long getCounter(String s) throws TException {
    return 0;
  }

  @Override
  public void setOption(String s, String s2) throws TException {
  }

  @Override
  public String getOption(String s) throws TException {
    return null;
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    return null;
  }

  @Override
  public String getCpuProfile(int i) throws TException {
    return null;
  }

  @Override
  public long aliveSince() throws TException {
    return 0;
  }

  @Override
  public void reinitialize() throws TException {
  }

  @Override
  public void shutdown() throws TException {
  }
}
