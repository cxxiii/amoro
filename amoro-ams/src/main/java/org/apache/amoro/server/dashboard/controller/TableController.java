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

package org.apache.amoro.server.dashboard.controller;

import static org.apache.amoro.properties.CatalogMetaProperties.CATALOG_TYPE_HIVE;

import io.javalin.http.Context;
import org.apache.amoro.Constants;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.client.OptimizingClientPools;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.hive.CachedHiveClientPool;
import org.apache.amoro.hive.HMSClientPool;
import org.apache.amoro.hive.catalog.MixedHiveCatalog;
import org.apache.amoro.hive.utils.HiveTableUtil;
import org.apache.amoro.hive.utils.UpgradeHiveTableUtil;
import org.apache.amoro.mixed.CatalogLoader;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.properties.CatalogMetaProperties;
import org.apache.amoro.properties.HiveTableProperties;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.dashboard.ServerTableDescriptor;
import org.apache.amoro.server.dashboard.ServerTableProperties;
import org.apache.amoro.server.dashboard.model.HiveTableInfo;
import org.apache.amoro.server.dashboard.model.TableMeta;
import org.apache.amoro.server.dashboard.model.TableOperation;
import org.apache.amoro.server.dashboard.model.UpgradeHiveMeta;
import org.apache.amoro.server.dashboard.model.UpgradeRunningInfo;
import org.apache.amoro.server.dashboard.model.UpgradeStatus;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.server.dashboard.response.PageResult;
import org.apache.amoro.server.dashboard.utils.AmsUtil;
import org.apache.amoro.server.dashboard.utils.CommonUtil;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.shade.guava32.com.google.common.base.Function;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.descriptor.AMSColumnInfo;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableSummary;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.SnapshotRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 处理表相关请求的控制器类
 * 提供表的各种操作接口，包括获取表详情、优化信息、分区信息等
 */
public class TableController {
  private static final Logger LOG = LoggerFactory.getLogger(TableController.class);
  private static final long UPGRADE_INFO_EXPIRE_INTERVAL = 60 * 60 * 1000; // 升级信息过期时间间隔(1小时)

  private final CatalogManager catalogManager; // 目录管理器
  private final TableManager tableManager; // 表管理器
  private final ServerTableDescriptor tableDescriptor; // 表描述器
  private final Configurations serviceConfig; // 服务配置
  private final ConcurrentHashMap<TableIdentifier, UpgradeRunningInfo> upgradeRunningInfo =
      new ConcurrentHashMap<>(); // 存储表升级状态信息的Map
  private final ScheduledExecutorService tableUpgradeExecutor; // 表升级执行器

  /**
   * 构造函数
   * @param catalogManager 目录管理器
   * @param tableManager 表管理器
   * @param tableDescriptor 表描述器
   * @param serviceConfig 服务配置
   */
  public TableController(
      CatalogManager catalogManager,
      TableManager tableManager,
      ServerTableDescriptor tableDescriptor,
      Configurations serviceConfig) {
    this.catalogManager = catalogManager;
    this.tableManager = tableManager;
    this.tableDescriptor = tableDescriptor;
    this.serviceConfig = serviceConfig;
    this.tableUpgradeExecutor =
        Executors.newScheduledThreadPool(
            0,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("async-hive-table-upgrade-%d")
                .build());
  }

  /**
   * 获取表详情信息
   * @param ctx HTTP请求上下文
   */
  public void getTableDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");

    // 参数校验
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(database)
            && StringUtils.isNotBlank(tableName),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkState(catalogManager.catalogExist(catalog), "invalid catalog!");

    // 获取表详情元数据
    ServerTableMeta serverTableMeta =
        tableDescriptor.getTableDetail(
            TableIdentifier.of(catalog, database, tableName).buildTableIdentifier());
    TableSummary tableSummary = serverTableMeta.getTableSummary();

    // 获取表运行时信息并设置优化状态
    Optional<ServerTableIdentifier> serverTableIdentifier =
        Optional.ofNullable(
            tableManager.getServerTableIdentifier(
                TableIdentifier.of(catalog, database, tableName).buildTableIdentifier()));
    if (serverTableIdentifier.isPresent()) {
      TableRuntimeMeta tableRuntimeMeta =
          tableManager.getTableRuntimeMata(serverTableIdentifier.get());
      if (tableRuntimeMeta != null) {
        tableSummary.setOptimizingStatus(tableRuntimeMeta.getTableStatus().name());
        AbstractOptimizingEvaluator.PendingInput tableRuntimeSummary =
            tableRuntimeMeta.getTableSummary();
        if (tableRuntimeSummary != null) {
          tableSummary.setHealthScore(tableRuntimeSummary.getHealthScore());
        }
      }
    } else {
      tableSummary.setOptimizingStatus(OptimizingStatus.IDLE.name());
    }
    ctx.json(OkResponse.of(serverTableMeta));
  }

  /**
   * 获取Hive表详情信息
   * @param ctx HTTP请求上下文
   */
  public void getHiveTableDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");

    // 获取HMS客户端连接池
    ServerCatalog serverCatalog = catalogManager.getServerCatalog(catalog);
    CatalogMeta catalogMeta = serverCatalog.getMetadata();
    TableMetaStore tableMetaStore = CatalogUtil.buildMetaStore(catalogMeta);
    HMSClientPool hmsClientPool =
        new CachedHiveClientPool(tableMetaStore, catalogMeta.getCatalogProperties());

    // 加载Hive表信息并转换为前端需要的格式
    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    HiveTableInfo hiveTableInfo;
    Table hiveTable = HiveTableUtil.loadHmsTable(hmsClientPool, tableIdentifier);
    List<AMSColumnInfo> schema = transformHiveSchemaToAMSColumnInfo(hiveTable.getSd().getCols());
    List<AMSColumnInfo> partitionColumnInfos =
        transformHiveSchemaToAMSColumnInfo(hiveTable.getPartitionKeys());
    hiveTableInfo =
        new HiveTableInfo(
            tableIdentifier,
            TableMeta.TableType.HIVE,
            schema,
            partitionColumnInfos,
            new HashMap<>(),
            hiveTable.getCreateTime());
    ctx.json(OkResponse.of(hiveTableInfo));
  }

  /**
   * 将Hive表升级为Mixed-Hive表
   * @param ctx HTTP请求上下文
   */
  public void upgradeHiveTable(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    UpgradeHiveMeta upgradeHiveMeta = ctx.bodyAsClass(UpgradeHiveMeta.class);

    // 获取目录元数据并配置AMS URI
    ServerCatalog serverCatalog = catalogManager.getServerCatalog(catalog);
    CatalogMeta catalogMeta = serverCatalog.getMetadata();
    String amsUri = AmsUtil.getAMSThriftAddress(serviceConfig, Constants.THRIFT_TABLE_SERVICE_NAME);
    catalogMeta.putToCatalogProperties(CatalogMetaProperties.AMS_URI, amsUri);
    TableMetaStore tableMetaStore = CatalogUtil.buildMetaStore(catalogMeta);

    // 检查目录是否支持MIXED_HIVE格式
    Set<TableFormat> tableFormats = CatalogUtil.tableFormats(catalogMeta);
    Preconditions.checkState(
        tableFormats.contains(TableFormat.MIXED_HIVE),
        "Catalog %s does not support MIXED_HIVE format",
        catalog);

    // 配置目录属性，只保留MIXED_HIVE格式
    Map<String, String> originCatalogProperties = catalogMeta.getCatalogProperties();
    Map<String, String> catalogProperties = new HashMap<>(originCatalogProperties);
    catalogProperties.put(CatalogMetaProperties.TABLE_FORMATS, TableFormat.MIXED_HIVE.name());

    // 创建MixedHiveCatalog实例
    MixedHiveCatalog mixedHiveCatalog =
        (MixedHiveCatalog)
            CatalogLoader.createCatalog(
                catalog, catalogMeta.getCatalogType(), catalogProperties, tableMetaStore);

    // 异步执行表升级任务
    tableUpgradeExecutor.execute(
        () -> {
          TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
          upgradeRunningInfo.put(tableIdentifier, new UpgradeRunningInfo());
          try {
            UpgradeHiveTableUtil.upgradeHiveTable(
                mixedHiveCatalog,
                tableIdentifier,
                upgradeHiveMeta.getPkList().stream()
                    .map(UpgradeHiveMeta.PrimaryKeyField::getFieldName)
                    .collect(Collectors.toList()),
                upgradeHiveMeta.getProperties());
            upgradeRunningInfo.get(tableIdentifier).setStatus(UpgradeStatus.SUCCESS.toString());
          } catch (Throwable t) {
            LOG.error("Failed to upgrade hive table to mixed-hive table ", t);
            upgradeRunningInfo.get(tableIdentifier).setErrorMessage(AmsUtil.getStackTrace(t));
            upgradeRunningInfo.get(tableIdentifier).setStatus(UpgradeStatus.FAILED.toString());
          } finally {
            // 设置升级信息过期时间
            tableUpgradeExecutor.schedule(
                () -> upgradeRunningInfo.remove(tableIdentifier),
                UPGRADE_INFO_EXPIRE_INTERVAL,
                TimeUnit.MILLISECONDS);
          }
        });
    ctx.json(OkResponse.ok());
  }

  /**
   * 获取表升级状态
   * @param ctx HTTP请求上下文
   */
  public void getUpgradeStatus(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    UpgradeRunningInfo info =
        upgradeRunningInfo.containsKey(TableIdentifier.of(catalog, db, table))
            ? upgradeRunningInfo.get(TableIdentifier.of(catalog, db, table))
            : new UpgradeRunningInfo(UpgradeStatus.NONE.toString());
    ctx.json(OkResponse.of(info));
  }

  /**
   * 获取Hive表升级为Mixed-Hive表所需的属性
   * @param ctx HTTP请求上下文
   * @throws IllegalAccessException 如果访问属性时出错
   */
  public void getUpgradeHiveTableProperties(Context ctx) throws IllegalAccessException {
    Map<String, String> keyValues = new TreeMap<>();
    // 获取表属性
    Map<String, String> tableProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(TableProperties.class);
    tableProperties.keySet().stream()
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(tableProperties.get(key), tableProperties.get(key + "_DEFAULT")));
    ServerTableProperties.HIDDEN_EXPOSED.forEach(keyValues::remove);

    // 获取Hive表属性
    Map<String, String> hiveProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(HiveTableProperties.class);
    hiveProperties.keySet().stream()
        .filter(key -> HiveTableProperties.EXPOSED.contains(hiveProperties.get(key)))
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(hiveProperties.get(key), hiveProperties.get(key + "_DEFAULT")));
    ctx.json(OkResponse.of(keyValues));
  }

  /**
   * 获取表的优化进程列表
   * @param ctx HTTP请求上下文
   */
  public void getOptimizingProcesses(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String type = ctx.queryParam("type");

    if (StringUtils.isBlank(type)) {
      type = null;
    }

    String status = ctx.queryParam("status");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    int offset = (page - 1) * pageSize;
    int limit = pageSize;
    Preconditions.checkArgument(offset >= 0, "offset[%s] must >= 0", offset);
    Preconditions.checkArgument(limit >= 0, "limit[%s] must >= 0", limit);

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    ProcessStatus processStatus =
        StringUtils.isBlank(status) ? null : ProcessStatus.valueOf(status);

    // 获取优化进程信息
    Pair<List<OptimizingProcessInfo>, Integer> optimizingProcessesInfo =
        tableDescriptor.getOptimizingProcessesInfo(
            tableIdentifier.buildTableIdentifier(), type, processStatus, limit, offset);
    List<OptimizingProcessInfo> result = optimizingProcessesInfo.getLeft();
    int total = optimizingProcessesInfo.getRight();

    ctx.json(OkResponse.of(PageResult.of(result, total)));
  }

  /**
   * 获取表的优化类型
   * @param ctx HTTP请求上下文
   */
  public void getOptimizingTypes(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);

    Map<String, String> values =
        tableDescriptor.getTableOptimizingTypes(tableIdentifier.buildTableIdentifier());
    ctx.json(OkResponse.of(values));
  }

  /**
   * 获取优化进程的任务列表
   * @param ctx HTTP请求上下文
   */
  public void getOptimizingProcessTasks(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String processId = ctx.pathParam("processId");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    int offset = (page - 1) * pageSize;
    int limit = pageSize;
    Preconditions.checkArgument(offset >= 0, "offset[%s] must >= 0", offset);
    Preconditions.checkArgument(limit >= 0, "limit[%s] must >= 0", limit);

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    List<OptimizingTaskInfo> optimizingTaskInfos =
        tableDescriptor.getOptimizingProcessTaskInfos(
            tableIdentifier.buildTableIdentifier(), processId);

    PageResult<OptimizingTaskInfo> pageResult = PageResult.of(optimizingTaskInfos, offset, limit);
    ctx.json(OkResponse.of(pageResult));
  }

  /**
   * 获取表的快照列表
   * @param ctx HTTP请求上下文
   */
  public void getTableSnapshots(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    // ref表示标签/分支
    String ref = ctx.queryParamAsClass("ref", String.class).getOrDefault(null);
    String operation =
        ctx.queryParamAsClass("operation", String.class)
            .getOrDefault(OperationType.ALL.displayName());
    OperationType operationType = OperationType.of(operation);

    List<AmoroSnapshotsOfTable> snapshotsOfTables =
        tableDescriptor.getSnapshots(
            TableIdentifier.of(catalog, database, tableName).buildTableIdentifier(),
            ref,
            operationType);
    int offset = (page - 1) * pageSize;
    PageResult<AmoroSnapshotsOfTable> pageResult =
        PageResult.of(snapshotsOfTables, offset, pageSize);
    ctx.json(OkResponse.of(pageResult));
  }

  /**
   * 获取快照详情
   * @param ctx HTTP请求上下文
   */
  public void getSnapshotDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    String snapshotId = ctx.pathParam("snapshotId");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    String ref = ctx.queryParamAsClass("ref", String.class).getOrDefault(null);

    List<PartitionFileBaseInfo> result =
        tableDescriptor.getSnapshotDetail(
            TableIdentifier.of(catalog, database, tableName).buildTableIdentifier(),
            snapshotId,
            ref);
    int offset = (page - 1) * pageSize;
    PageResult<PartitionFileBaseInfo> amsPageResult = PageResult.of(result, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取表的分区列表
   * @param ctx HTTP请求上下文
   */
  public void getTablePartitions(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String filter = ctx.queryParamAsClass("filter", String.class).getOrDefault("");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionBaseInfo> partitionBaseInfos =
        tableDescriptor.getTablePartition(
            TableIdentifier.of(catalog, database, table).buildTableIdentifier());
    // 过滤和排序分区信息
    partitionBaseInfos =
        partitionBaseInfos.stream()
            .filter(e -> e.getPartition().contains(filter))
            .sorted(Comparator.comparing(PartitionBaseInfo::getPartition).reversed())
            .collect(Collectors.toList());
    int offset = (page - 1) * pageSize;
    PageResult<PartitionBaseInfo> amsPageResult =
        PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取分区文件列表信息
   * @param ctx HTTP请求上下文
   */
  public void getPartitionFileListInfo(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String partition = ctx.pathParam("partition");

    Integer specId = ctx.queryParamAsClass("specId", Integer.class).getOrDefault(0);
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionFileBaseInfo> partitionFileBaseInfos =
        tableDescriptor.getTableFile(
            TableIdentifier.of(catalog, db, table).buildTableIdentifier(), partition, specId);
    int offset = (page - 1) * pageSize;
    PageResult<PartitionFileBaseInfo> amsPageResult =
        PageResult.of(partitionFileBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取表操作记录
   * @param ctx HTTP请求上下文
   * @throws Exception 如果获取操作记录时出错
   */
  public void getTableOperations(Context ctx) throws Exception {
    String catalogName = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");

    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    int offset = (page - 1) * pageSize;

    List<DDLInfo> ddlInfoList =
        tableDescriptor.getTableOperations(
            TableIdentifier.of(catalogName, db, tableName).buildTableIdentifier());
    Collections.reverse(ddlInfoList);
    PageResult<TableOperation> amsPageResult =
        PageResult.of(ddlInfoList, offset, pageSize, TableOperation::buildFromDDLInfo);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取目录下数据库的表列表
   * @param ctx HTTP请求上下文
   */
  public void getTableList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String keywords = ctx.queryParam("keywords");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(db),
        "catalog.database can not be empty in any element");

    ServerCatalog serverCatalog = catalogManager.getServerCatalog(catalog);
    // 表格式到类型的转换函数
    Function<TableFormat, String> formatToType =
        format -> {
          if (format.equals(TableFormat.MIXED_HIVE) || format.equals(TableFormat.MIXED_ICEBERG)) {
            return TableMeta.TableType.ARCTIC.toString();
          } else if (format.equals(TableFormat.PAIMON)) {
            return TableMeta.TableType.PAIMON.toString();
          } else if (format.equals(TableFormat.ICEBERG)) {
            return TableMeta.TableType.ICEBERG.toString();
          } else if (format.equals(TableFormat.HUDI)) {
            return TableMeta.TableType.HUDI.toString();
          } else {
            return format.toString();
          }
        };

    // 获取表列表并按格式和名称排序
    List<TableMeta> tables =
        serverCatalog.listTables(db).stream()
            .map(
                idWithFormat ->
                    new TableMeta(
                        idWithFormat.getIdentifier().getTableName(),
                        formatToType.apply(idWithFormat.getTableFormat())))
            .sorted(
                (table1, table2) -> {
                  if (Objects.equals(table1.getType(), table2.getType())) {
                    return table1.getName().compareTo(table2.getName());
                  } else {
                    return table1.getType().compareTo(table2.getType());
                  }
                })
            .collect(Collectors.toList());

    // 如果是Hive目录，还需要获取Hive表
    String catalogType = serverCatalog.getMetadata().getCatalogType();
    if (catalogType.equals(CATALOG_TYPE_HIVE)) {
      CatalogMeta catalogMeta = serverCatalog.getMetadata();
      TableMetaStore tableMetaStore = CatalogUtil.buildMetaStore(catalogMeta);
      HMSClientPool hmsClientPool =
          new CachedHiveClientPool(tableMetaStore, catalogMeta.getCatalogProperties());

      List<String> hiveTables = HiveTableUtil.getAllHiveTables(hmsClientPool, db);
      Set<String> mixedHiveTables =
          tables.stream().map(TableMeta::getName).collect(Collectors.toSet());
      hiveTables.stream()
          .filter(e -> !mixedHiveTables.contains(e))
          .sorted(String::compareTo)
          .forEach(e -> tables.add(new TableMeta(e, TableMeta.TableType.HIVE.toString())));
    }

    // 根据关键词过滤结果
    ctx.json(
        OkResponse.of(
            tables.stream()
                .filter(t -> StringUtils.isBlank(keywords) || t.getName().contains(keywords))
                .collect(Collectors.toList())));
  }

  /**
   * 获取目录下的数据库列表
   * @param ctx HTTP请求上下文
   */
  public void getDatabaseList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String keywords = ctx.queryParam("keywords");

    List<String> dbList =
        catalogManager.getServerCatalog(catalog).listDatabases().stream()
            .filter(item -> StringUtils.isBlank(keywords) || item.contains(keywords))
            .collect(Collectors.toList());
    ctx.json(OkResponse.of(dbList));
  }

  /**
   * 获取目录列表
   * @param ctx HTTP请求上下文
   */
  public void getCatalogs(Context ctx) {
    List<CatalogMeta> catalogs = catalogManager.listCatalogMetas();
    ctx.json(OkResponse.of(catalogs));
  }

  /**
   * 获取表详情页面的查询令牌
   * @param ctx HTTP请求上下文
   */
  public void getTableDetailTabToken(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");

    String signCal = CommonUtil.generateTablePageToken(catalog, db, table);
    ctx.json(OkResponse.of(signCal));
  }

  /**
   * 获取表的标签列表
   * @param ctx HTTP请求上下文
   */
  public void getTableTags(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    List<TagOrBranchInfo> partitionBaseInfos =
        tableDescriptor.getTableTags(
            TableIdentifier.of(catalog, database, table).buildTableIdentifier());
    int offset = (page - 1) * pageSize;
    PageResult<TagOrBranchInfo> amsPageResult = PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取表的分支列表
   * @param ctx HTTP请求上下文
   */
  public void getTableBranches(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    List<TagOrBranchInfo> partitionBaseInfos =
        tableDescriptor.getTableBranches(
            TableIdentifier.of(catalog, database, table).buildTableIdentifier());
    putMainBranchFirst(partitionBaseInfos);
    int offset = (page - 1) * pageSize;
    PageResult<TagOrBranchInfo> amsPageResult = PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 获取表的消费者信息列表
   * @param ctx HTTP请求上下文
   */
  public void getTableConsumerInfos(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    List<ConsumerInfo> consumerInfos =
        tableDescriptor.getTableConsumersInfos(
            TableIdentifier.of(catalog, database, table).buildTableIdentifier());
    int offset = (page - 1) * pageSize;
    PageResult<ConsumerInfo> amsPageResult = PageResult.of(consumerInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * 取消表的优化进程
   * @param ctx HTTP请求上下文
   */
  public void cancelOptimizingProcess(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String processIds = ctx.pathParam("processId");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkState(catalogManager.catalogExist(catalog), "invalid catalog!");
    long processId = Long.parseLong(processIds);

    // 获取表运行时元数据
    ServerTableIdentifier serverTableIdentifier =
        tableManager.getServerTableIdentifier(
            TableIdentifier.of(catalog, db, table).buildTableIdentifier());
    TableRuntimeMeta meta = tableManager.getTableRuntimeMata(serverTableIdentifier);
    if (meta == null || meta.getOptimizingProcessId() != processId) {
      throw new IllegalArgumentException(
          String.format("Can't cancel optimizing process %s", processId));
    }

    // 调用优化服务取消进程
    OptimizingService.Iface client =
        OptimizingClientPools.getClient(
            AmsUtil.getAMSThriftAddress(serviceConfig, Constants.THRIFT_OPTIMIZING_SERVICE_NAME));
    try {
      client.cancelProcess(processId);
    } catch (TException e) {
      throw new IllegalStateException("Failed to cancel optimizing process:" + e.getMessage());
    }
    ctx.json(OkResponse.ok());
  }

  /**
   * 将主分支放在分支列表的第一位
   * @param branchInfos 分支信息列表
   */
  private void putMainBranchFirst(List<TagOrBranchInfo> branchInfos) {
    if (branchInfos.size() <= 1) {
      return;
    }
    branchInfos.stream()
        .filter(branch -> SnapshotRef.MAIN_BRANCH.equals(branch.getName()))
        .findFirst()
        .ifPresent(
            mainBranch -> {
              branchInfos.remove(mainBranch);
              branchInfos.add(0, mainBranch);
            });
  }

  /**
   * 将Hive表模式转换为AMS列信息
   * @param fields Hive表字段列表
   * @return AMS列信息列表
   */
  private List<AMSColumnInfo> transformHiveSchemaToAMSColumnInfo(List<FieldSchema> fields) {
    return fields.stream()
        .map(
            f -> {
              AMSColumnInfo columnInfo = new AMSColumnInfo();
              columnInfo.setField(f.getName());
              columnInfo.setType(f.getType());
              columnInfo.setComment(f.getComment());
              return columnInfo;
            })
        .collect(Collectors.toList());
  }
}
