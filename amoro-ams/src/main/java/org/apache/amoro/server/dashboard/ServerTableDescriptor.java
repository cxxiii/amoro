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

package org.apache.amoro.server.dashboard;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.util.ThreadPools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

/**
 * 服务端表描述器类，用于管理和提供表的各种元数据信息
 * 继承自PersistentBase，提供持久化能力
 */
public class ServerTableDescriptor extends PersistentBase {

  // 存储不同表格式对应的描述器映射
  private final Map<TableFormat, FormatTableDescriptor> formatDescriptorMap = new HashMap<>();

  private final CatalogManager catalogManager;  // 目录管理器
  private final TableManager tableManager;      // 表管理器

  /**
   * 构造函数
   * @param catalogManager 目录管理器实例
   * @param tableManager 表管理器实例
   * @param serviceConfig 服务配置
   */
  public ServerTableDescriptor(
      CatalogManager catalogManager, TableManager tableManager, Configurations serviceConfig) {
    this.tableManager = tableManager;
    this.catalogManager = catalogManager;

    // 所有表格式共享名为iceberg-worker-pool-%d的工作线程池
    ExecutorService executorService = ThreadPools.getWorkerPool();
    // 使用ServiceLoader加载所有FormatTableDescriptor实现
    ServiceLoader<FormatTableDescriptor> tableDescriptorLoader =
        ServiceLoader.load(FormatTableDescriptor.class);
    for (FormatTableDescriptor descriptor : tableDescriptorLoader) {
      // 注册描述器支持的所有表格式
      for (TableFormat format : descriptor.supportFormat()) {
        formatDescriptorMap.put(format, descriptor);
      }
      // 为描述器设置IO执行器
      descriptor.withIoExecutor(executorService);
    }
  }

  /**
   * 获取表详情信息
   * @param tableIdentifier 表标识符
   * @return 表元数据信息
   */
  public ServerTableMeta getTableDetail(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableDetail(amoroTable);
  }

  /**
   * 获取表快照列表
   * @param tableIdentifier 表标识符
   * @param ref 引用名称（如分支或标签）
   * @param operationType 操作类型
   * @return 快照列表
   */
  public List<AmoroSnapshotsOfTable> getSnapshots(
      TableIdentifier tableIdentifier, String ref, OperationType operationType) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getSnapshots(amoroTable, ref, operationType);
  }

  /**
   * 获取快照详情
   * @param tableIdentifier 表标识符
   * @param snapshotId 快照ID
   * @param ref 引用名称
   * @return 分区文件基本信息列表
   */
  public List<PartitionFileBaseInfo> getSnapshotDetail(
      TableIdentifier tableIdentifier, String snapshotId, String ref) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getSnapshotDetail(amoroTable, snapshotId, ref);
  }

  /**
   * 获取表操作记录
   * @param tableIdentifier 表标识符
   * @return DDL操作信息列表
   */
  public List<DDLInfo> getTableOperations(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableOperations(amoroTable);
  }

  /**
   * 获取表分区信息
   * @param tableIdentifier 表标识符
   * @return 分区基本信息列表
   */
  public List<PartitionBaseInfo> getTablePartition(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTablePartitions(amoroTable);
  }

  /**
   * 获取表文件信息
   * @param tableIdentifier 表标识符
   * @param partition 分区名称
   * @param specId 规格ID
   * @return 分区文件基本信息列表
   */
  public List<PartitionFileBaseInfo> getTableFile(
      TableIdentifier tableIdentifier, String partition, Integer specId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableFiles(amoroTable, partition, specId);
  }

  /**
   * 获取表标签信息
   * @param tableIdentifier 表标识符
   * @return 标签或分支信息列表
   */
  public List<TagOrBranchInfo> getTableTags(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableTags(amoroTable);
  }

  /**
   * 获取表分支信息
   * @param tableIdentifier 表标识符
   * @return 分支信息列表
   */
  public List<TagOrBranchInfo> getTableBranches(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableBranches(amoroTable);
  }

  /**
   * 获取表消费者信息
   * @param tableIdentifier 表标识符
   * @return 消费者信息列表
   */
  public List<ConsumerInfo> getTableConsumersInfos(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableConsumerInfos(amoroTable);
  }

  /**
   * 获取表优化过程信息
   * @param tableIdentifier 表标识符
   * @param type 优化类型
   * @param status 过程状态
   * @param limit 返回数量限制
   * @param offset 偏移量
   * @return 包含优化过程信息列表和总数的Pair对象
   */
  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      TableIdentifier tableIdentifier, String type, ProcessStatus status, int limit, int offset) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getOptimizingProcessesInfo(
        amoroTable, type, status, limit, offset);
  }

  /**
   * 获取优化过程任务信息
   * @param tableIdentifier 表标识符
   * @param processId 过程ID
   * @return 优化任务信息列表
   */
  public List<OptimizingTaskInfo> getOptimizingProcessTaskInfos(
      TableIdentifier tableIdentifier, String processId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getOptimizingTaskInfos(amoroTable, processId);
  }

  /**
   * 获取表优化类型
   * @param tableIdentifier 表标识符
   * @return 优化类型映射表
   */
  public Map<String, String> getTableOptimizingTypes(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    FormatTableDescriptor formatTableDescriptor = formatDescriptorMap.get(amoroTable.format());
    return formatTableDescriptor.getTableOptimizingTypes(amoroTable);
  }

  /**
   * 加载表对象
   * @param identifier 表标识符
   * @return AmoroTable对象
   */
  private AmoroTable<?> loadTable(TableIdentifier identifier) {
    ServerCatalog catalog = catalogManager.getServerCatalog(identifier.getCatalog());
    return catalog.loadTable(identifier.getDatabase(), identifier.getTableName());
  }
}
