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

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.dashboard.model.OverviewDataSizeItem;
import org.apache.amoro.server.dashboard.model.OverviewResourceUsageItem;
import org.apache.amoro.server.dashboard.model.OverviewTopTableItem;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.CatalogMetaMapper;
import org.apache.amoro.server.persistence.mapper.OptimizerMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableList;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 概览管理器，用于管理和维护系统概览数据
 * 包括表信息、资源使用情况、数据大小等统计信息
 */
public class OverviewManager extends PersistentBase {

  // 优化状态常量定义
  public static final String STATUS_PENDING = "Pending";
  public static final String STATUS_PLANING = "Planing";
  public static final String STATUS_EXECUTING = "Executing";
  public static final String STATUS_IDLE = "Idle";
  public static final String STATUS_COMMITTING = "Committing";

  private static final Logger LOG = LoggerFactory.getLogger(OverviewManager.class);
  private final List<OverviewTopTableItem> allTopTableItem = new ArrayList<>(); // 存储所有表项信息
  private final Map<String, Long> optimizingStatusCountMap = new ConcurrentHashMap<>(); // 优化状态统计
  private final ConcurrentLinkedDeque<OverviewResourceUsageItem> resourceUsageHistory = // 资源使用历史记录
      new ConcurrentLinkedDeque<>();
  private final ConcurrentLinkedDeque<OverviewDataSizeItem> dataSizeHistory = // 数据大小历史记录
      new ConcurrentLinkedDeque<>();
  private final AtomicInteger totalCatalog = new AtomicInteger(); // 总目录数
  private final AtomicLong totalDataSize = new AtomicLong(); // 总数据大小
  private final AtomicInteger totalTableCount = new AtomicInteger(); // 总表数量
  private final AtomicInteger totalCpu = new AtomicInteger(); // 总CPU资源
  private final AtomicLong totalMemory = new AtomicLong(); // 总内存资源

  private final int maxRecordCount; // 最大记录数

  /**
   * 构造函数，基于服务器配置初始化
   * @param serverConfigs 服务器配置
   */
  public OverviewManager(Configurations serverConfigs) {
    this(
        serverConfigs.getInteger(AmoroManagementConf.OVERVIEW_CACHE_MAX_SIZE),
        serverConfigs.get(AmoroManagementConf.OVERVIEW_CACHE_REFRESH_INTERVAL));
  }

  /**
   * 测试用构造函数
   * @param maxRecordCount 最大记录数
   * @param refreshInterval 刷新间隔
   */
  @VisibleForTesting
  public OverviewManager(int maxRecordCount, Duration refreshInterval) {
    this.maxRecordCount = maxRecordCount;
    ScheduledExecutorService overviewUpdaterScheduler =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("overview-refresh-scheduler-%d")
                .setDaemon(true)
                .build());
    resetStatusMap();

    if (refreshInterval.toMillis() > 0) {
      overviewUpdaterScheduler.scheduleAtFixedRate(
          this::refresh, 1000L, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  // 以下是一系列getter方法，用于获取各类统计信息
  public List<OverviewTopTableItem> getAllTopTableItem() {
    return ImmutableList.copyOf(allTopTableItem);
  }

  public int getTotalCatalog() {
    return totalCatalog.get();
  }

  public int getTotalTableCount() {
    return totalTableCount.get();
  }

  public long getTotalDataSize() {
    return totalDataSize.get();
  }

  public int getTotalCpu() {
    return totalCpu.get();
  }

  public long getTotalMemory() {
    return totalMemory.get();
  }

  /**
   * 获取指定时间点之后的资源使用历史
   * @param startTime 起始时间戳
   * @return 资源使用历史列表
   */
  public List<OverviewResourceUsageItem> getResourceUsageHistory(long startTime) {
    return resourceUsageHistory.stream()
        .filter(item -> item.getTs() >= startTime)
        .collect(Collectors.toList());
  }

  /**
   * 获取指定时间点之后的数据大小历史
   * @param startTime 起始时间戳
   * @return 数据大小历史列表
   */
  public List<OverviewDataSizeItem> getDataSizeHistory(long startTime) {
    return dataSizeHistory.stream()
        .filter(item -> item.getTs() >= startTime)
        .collect(Collectors.toList());
  }

  /**
   * 获取优化状态统计
   * @return 优化状态统计映射表
   */
  public Map<String, Long> getOptimizingStatus() {
    return optimizingStatusCountMap;
  }

  /**
   * 刷新概览数据缓存
   */
  @VisibleForTesting
  public void refresh() {
    long start = System.currentTimeMillis();
    LOG.info("Refreshing overview cache");
    try {
      refreshTableCache(start);
      refreshResourceUsage(start);

    } catch (Exception e) {
      LOG.error("Refreshed overview cache failed", e);
    } finally {
      long end = System.currentTimeMillis();
      LOG.info("Refreshed overview cache in {} ms.", end - start);
    }
  }

  /**
   * 刷新表缓存数据
   * 该方法用于更新系统概览中的表相关统计信息，包括：
   * 1. 表总数、数据总量、文件总数等基础统计
   * 2. 各表的详细信息（大小、文件数等）
   * 3. 表的优化状态统计
   *
   * @param ts 时间戳，用于记录数据刷新时间
   */
  private void refreshTableCache(long ts) {
    // 获取总目录数
    // 通过MyBatis框架调用CatalogMetaMapper接口中的selectCatalogCount()方法，查询数据库中的目录(catalog)总数。
    int totalCatalogs = getAs(CatalogMetaMapper.class, CatalogMetaMapper::selectCatalogCount);

    // 获取所有表的运行时元数据
    List<TableRuntimeMeta> metas =
        getAs(TableMetaMapper.class, TableMetaMapper::selectTableRuntimeMetas);
    // 初始化统计变量
    AtomicLong totalDataSize = new AtomicLong();  // 总数据大小
    AtomicInteger totalFileCounts = new AtomicInteger();  // 总文件数
    Map<String, OverviewTopTableItem> topTableItemMap = Maps.newHashMap();  // 表项映射表
    Map<String, Long> optimizingStatusMap = Maps.newHashMap();  // 优化状态统计映射表

    // 遍历所有表元数据
    for (TableRuntimeMeta meta : metas) {
      // 将元数据转换为概览表项
      // 调用toTopTableItem方法，将TableRuntimeMeta类型的表元数据转换为OverviewTopTableItem类型的概览表项
      // 返回的是一个Optional对象，表示转换结果可能存在也可能不存在
      Optional<OverviewTopTableItem> optItem = toTopTableItem(meta);
      optItem.ifPresent(
          tableItem -> {
            // 将表项加入映射表并更新统计
            topTableItemMap.put(tableItem.getTableName(), tableItem);
            // 原子操作，用于累加表的数据大小到总数据量统计中
            totalDataSize.addAndGet(tableItem.getTableSize());
            totalFileCounts.addAndGet(tableItem.getFileCount());
          });

      // 统计优化状态
      String status = statusToMetricString(meta.getTableStatus());
      if (StringUtils.isNotEmpty(status)) {
        optimizingStatusMap.putIfAbsent(status, 0L);
        // 原子操作，用于更新Map中指定key的value值
        optimizingStatusMap.computeIfPresent(status, (k, v) -> v + 1);
      }
    }

    // 更新类成员变量
    this.totalCatalog.set(totalCatalogs);
    this.totalTableCount.set(topTableItemMap.size());
    this.totalDataSize.set(totalDataSize.get());
    this.allTopTableItem.clear();
    this.allTopTableItem.addAll(topTableItemMap.values());
    // 记录数据大小历史
    addAndCheck(new OverviewDataSizeItem(ts, this.totalDataSize.get()));
    // 重置并更新优化状态统计
    resetStatusMap();
    this.optimizingStatusCountMap.putAll(optimizingStatusMap);
  }

  /**
   * 将表运行时元数据转换为概览表项
   *
   * @param meta 表运行时元数据对象，包含表的详细信息
   * @return Optional包装的OverviewTopTableItem对象，如果输入为null则返回空Optional
   */
  private Optional<OverviewTopTableItem> toTopTableItem(TableRuntimeMeta meta) {
    // 检查输入参数是否为null
    if (meta == null) {
      return Optional.empty();
    }
    // 创建新的概览表项对象，使用完整表名初始化
    OverviewTopTableItem tableItem = new OverviewTopTableItem(fullTableName(meta));
    // 如果表摘要信息存在，则设置表大小、文件数和健康分数
    if (meta.getTableSummary() != null) {
      tableItem.setTableSize(meta.getTableSummary().getTotalFileSize());
      tableItem.setFileCount(meta.getTableSummary().getTotalFileCount());
      tableItem.setHealthScore(meta.getTableSummary().getHealthScore());
    }
    // 计算平均文件大小（总大小/文件数，避免除以0）
    tableItem.setAverageFileSize(
        tableItem.getFileCount() == 0 ? 0 : tableItem.getTableSize() / tableItem.getFileCount());
    // 返回包装后的表项对象
    return Optional.of(tableItem);
  }

  /**
   * 将优化状态转换为度量字符串
   * @param status 优化状态
   * @return 对应的度量字符串
   */
  private String statusToMetricString(OptimizingStatus status) {
    if (status == null) {
      return null;
    }
    switch (status) {
      case PENDING:
        return STATUS_PENDING;
      case PLANNING:
        return STATUS_PLANING;
      case MINOR_OPTIMIZING:
      case MAJOR_OPTIMIZING:
      case FULL_OPTIMIZING:
        return STATUS_EXECUTING;
      case IDLE:
        return STATUS_IDLE;
      case COMMITTING:
        return STATUS_COMMITTING;
      default:
        return null;
    }
  }

  /**
   * 重置状态映射表
   */
  private void resetStatusMap() {
    optimizingStatusCountMap.clear();
    optimizingStatusCountMap.put(STATUS_PENDING, 0L);
    optimizingStatusCountMap.put(STATUS_PLANING, 0L);
    optimizingStatusCountMap.put(STATUS_EXECUTING, 0L);
    optimizingStatusCountMap.put(STATUS_IDLE, 0L);
    optimizingStatusCountMap.put(STATUS_COMMITTING, 0L);
  }

  /**
   * 刷新资源使用情况统计
   * 该方法用于计算并更新系统中所有优化器实例的CPU和内存资源使用总量
   *
   * @param ts 时间戳，用于记录资源使用数据的时间点
   */
  private void refreshResourceUsage(long ts) {
    // 获取所有优化器实例列表
    List<OptimizerInstance> instances = getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
    // 初始化CPU计数器和内存计数器
    AtomicInteger cpuCount = new AtomicInteger();  // 总CPU线程数
    AtomicLong memoryBytes = new AtomicLong();     // 总内存字节数

    // 遍历所有优化器实例，累加资源使用量
    for (OptimizerInstance instance : instances) {
      cpuCount.addAndGet(instance.getThreadCount());  // 累加CPU线程数
      memoryBytes.addAndGet(instance.getMemoryMb() * 1024L * 1024L);  // 将MB转换为字节并累加
    }

    // 更新类成员变量
    this.totalCpu.set(cpuCount.get());      // 设置总CPU数
    this.totalMemory.set(memoryBytes.get()); // 设置总内存数

    // 创建资源使用记录项并添加到历史队列
    addAndCheck(new OverviewResourceUsageItem(ts, cpuCount.get(), memoryBytes.get()));
  }

  /**
   * 添加数据大小项并检查大小限制
   * @param dataSizeItem 数据大小项
   */
  private void addAndCheck(OverviewDataSizeItem dataSizeItem) {
    dataSizeHistory.add(dataSizeItem);
    checkSize(dataSizeHistory);
  }

  /**
   * 添加资源使用项并检查大小限制
   * @param resourceUsageItem 资源使用项
   */
  private void addAndCheck(OverviewResourceUsageItem resourceUsageItem) {
    resourceUsageHistory.add(resourceUsageItem);
    checkSize(resourceUsageHistory);
  }

  /**
   * 检查队列大小，超过限制则移除最早记录
   * @param deque 待检查的队列
   * @param <T> 队列元素类型
   */
  private <T> void checkSize(Deque<T> deque) {
    if (deque.size() > maxRecordCount) {
      deque.poll();
    }
  }

  /**
   * 获取完整表名
   * @param meta 表运行时元数据
   * @return 完整表名(catalog.db.table)
   */
  private String fullTableName(TableRuntimeMeta meta) {
    return meta.getCatalogName()
        .concat(".")
        .concat(meta.getDbName())
        .concat(".")
        .concat(meta.getTableName());
  }
}
