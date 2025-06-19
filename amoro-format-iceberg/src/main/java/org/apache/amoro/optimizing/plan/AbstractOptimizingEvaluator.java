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

package org.apache.amoro.optimizing.plan;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.scan.IcebergTableFileScanHelper;
import org.apache.amoro.optimizing.scan.KeyedTableFileScanHelper;
import org.apache.amoro.optimizing.scan.TableFileScanHelper;
import org.apache.amoro.optimizing.scan.UnkeyedTableFileScanHelper;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.amoro.table.KeyedTableSnapshot;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableSnapshot;
import org.apache.amoro.utils.ExpressionUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// 导入相关依赖...

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 抽象优化评估器基类，用于评估表是否需要优化以及生成优化计划
 */
public abstract class AbstractOptimizingEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOptimizingEvaluator.class);

  // 表标识符
  protected final ServerTableIdentifier identifier;
  // 优化配置
  protected final OptimizingConfig config;
  // 混合表对象
  protected final MixedTable mixedTable;
  // 当前表快照
  protected final TableSnapshot currentSnapshot;
  // 上次全量优化时间
  protected final long lastFullOptimizingTime;
  // 上次增量优化时间
  protected final long lastMinorOptimizingTime;
  // 最大待处理分区数
  protected final int maxPendingPartitions;
  // 是否已初始化标志
  protected boolean isInitialized = false;
  // 需要优化的分区计划映射表
  protected Map<String, PartitionEvaluator> needOptimizingPlanMap = Maps.newHashMap();
  // 所有分区计划映射表
  protected Map<String, PartitionEvaluator> partitionPlanMap = Maps.newHashMap();

  /**
   * 构造函数
   * @param identifier 表标识符
   * @param config 优化配置
   * @param table 混合表对象
   * @param currentSnapshot 当前表快照
   * @param maxPendingPartitions 最大待处理分区数
   * @param lastMinorOptimizingTime 上次增量优化时间
   * @param lastFullOptimizingTime 上次全量优化时间
   */
  public AbstractOptimizingEvaluator(
      ServerTableIdentifier identifier,
      OptimizingConfig config,
      MixedTable table,
      TableSnapshot currentSnapshot,
      int maxPendingPartitions,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime) {
    this.identifier = identifier;
    this.config = config;
    this.mixedTable = table;
    this.currentSnapshot = currentSnapshot;
    this.maxPendingPartitions = maxPendingPartitions;
    this.lastFullOptimizingTime = lastFullOptimizingTime;
    this.lastMinorOptimizingTime = lastMinorOptimizingTime;
  }

  /**
   * 初始化评估器
   */
  protected void initEvaluator() {
    long startTime = System.currentTimeMillis();
    TableFileScanHelper tableFileScanHelper;
    // 根据表格式创建不同的文件扫描帮助类
    if (TableFormat.ICEBERG.equals(mixedTable.format())) {
      tableFileScanHelper =
          new IcebergTableFileScanHelper(mixedTable.asUnkeyedTable(), currentSnapshot.snapshotId());
    } else {
      if (mixedTable.isUnkeyedTable()) {
        tableFileScanHelper =
            new UnkeyedTableFileScanHelper(
                mixedTable.asUnkeyedTable(), currentSnapshot.snapshotId());
      } else {
        tableFileScanHelper =
            new KeyedTableFileScanHelper(
                mixedTable.asKeyedTable(), ((KeyedTableSnapshot) currentSnapshot));
      }
    }
    // 设置分区过滤器
    tableFileScanHelper.withPartitionFilter(getPartitionFilter());
    // 初始化分区计划
    initPartitionPlans(tableFileScanHelper);
    isInitialized = true;
    LOG.info(
        "{} finished evaluating, found {} partitions that need optimizing in {} ms",
        mixedTable.id(),
        needOptimizingPlanMap.size(),
        System.currentTimeMillis() - startTime);
  }

  /**
   * 获取分区过滤器表达式
   * @return 分区过滤表达式
   */
  protected Expression getPartitionFilter() {
    return ExpressionUtil.convertSqlFilterToIcebergExpression(
        config.getFilter(), mixedTable.schema().columns());
  }

  /**
   * 初始化分区计划
   * @param tableFileScanHelper 表文件扫描帮助类
   */
  private void initPartitionPlans(TableFileScanHelper tableFileScanHelper) {
    long startTime = System.currentTimeMillis();
    long count = 0;
    try (CloseableIterable<TableFileScanHelper.FileScanResult> results =
        tableFileScanHelper.scan()) {
      for (TableFileScanHelper.FileScanResult fileScanResult : results) {
        // 获取分区规格和分区数据
        PartitionSpec partitionSpec = tableFileScanHelper.getSpec(fileScanResult.file().specId());
        StructLike partition = fileScanResult.file().partition();
        String partitionPath = partitionSpec.partitionToPath(partition);
        // 创建或获取分区评估器
        PartitionEvaluator evaluator =
            partitionPlanMap.computeIfAbsent(
                partitionPath,
                ignore -> buildEvaluator(Pair.of(partitionSpec.specId(), partition)));
        // 添加文件到评估器
        evaluator.addFile(fileScanResult.file(), fileScanResult.deleteFiles());
        count++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    LOG.info(
        "{} finished file scanning, scanning {} files in {} ms",
        mixedTable.id(),
        count,
        System.currentTimeMillis() - startTime);
    // 筛选需要优化的分区并限制数量
    needOptimizingPlanMap.putAll(
        partitionPlanMap.entrySet().stream()
            .filter(entry -> entry.getValue().isNecessary())
            .limit(maxPendingPartitions)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /**
   * 构建分区评估器抽象方法
   * @param partition 分区信息对(规格ID,分区数据)
   * @return 分区评估器实例
   */
  protected abstract PartitionEvaluator buildEvaluator(Pair<Integer, StructLike> partition);

  /**
   * 判断是否需要优化
   * @return 如果需要优化返回true，否则false
   */
  public boolean isNecessary() {
    if (!isInitialized) {
      initEvaluator();
    }
    return !needOptimizingPlanMap.isEmpty();
  }

  /**
   * 获取待处理输入数据
   * @return 待处理输入对象
   */
  public PendingInput getPendingInput() {
    if (!isInitialized) {
      initEvaluator();
    }
    // Dangling delete files will cause the data scanned by TableScan
    // to be inconsistent with the snapshot summary of iceberg
    // 对于Iceberg格式表，处理悬挂删除文件导致的数据不一致问题
    if (TableFormat.ICEBERG == mixedTable.format()) {
      Snapshot snapshot = mixedTable.asUnkeyedTable().snapshot(currentSnapshot.snapshotId());
      return new PendingInput(partitionPlanMap.values(), snapshot);
    }

    return new PendingInput(partitionPlanMap.values());
  }

  /**
   * 获取优化待处理输入数据
   * @return 待处理输入对象
   */
  public PendingInput getOptimizingPendingInput() {
    if (!isInitialized) {
      initEvaluator();
    }
    return new PendingInput(needOptimizingPlanMap.values());
  }

  /**
   * 待处理输入数据类，用于统计优化相关信息
   */
  public static class PendingInput {

    // 分区映射表，key为规格ID，value为分区数据集合
    @JsonIgnore private final Map<Integer, Set<StructLike>> partitions = Maps.newHashMap();

    // 各种文件统计信息
    private int totalFileCount = 0;
    private long totalFileSize = 0L;
    private long totalFileRecords = 0L;
    private int dataFileCount = 0;
    private long dataFileSize = 0L;
    private long dataFileRecords = 0L;
    private int equalityDeleteFileCount = 0;
    private int positionalDeleteFileCount = 0;
    private long positionalDeleteBytes = 0L;
    private long equalityDeleteBytes = 0L;
    private long equalityDeleteFileRecords = 0L;
    private long positionalDeleteFileRecords = 0L;
    private int danglingDeleteFileCount = 0;
    private int healthScore = -1; // -1表示未计算

    public PendingInput() {}

    /**
     * 构造函数
     * @param evaluators 分区评估器集合
     */
    public PendingInput(Collection<PartitionEvaluator> evaluators) {
      initialize(evaluators);
      // 计算总文件数、大小和记录数
      totalFileCount = dataFileCount + positionalDeleteFileCount + equalityDeleteFileCount;
      totalFileSize = dataFileSize + positionalDeleteBytes + equalityDeleteBytes;
      totalFileRecords = dataFileRecords + positionalDeleteFileRecords + equalityDeleteFileRecords;
    }

    /**
     * 构造函数(针对Iceberg表)
     * @param evaluators 分区评估器集合
     * @param snapshot Iceberg快照
     */
    public PendingInput(Collection<PartitionEvaluator> evaluators, Snapshot snapshot) {
      initialize(evaluators);
      Map<String, String> summary = snapshot.summary();
      // 从快照摘要中获取统计信息
      int totalDeleteFiles =
          PropertyUtil.propertyAsInt(summary, SnapshotSummary.TOTAL_DELETE_FILES_PROP, 0);
      int totalDataFiles =
          PropertyUtil.propertyAsInt(summary, SnapshotSummary.TOTAL_DATA_FILES_PROP, 0);
      totalFileRecords =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.TOTAL_RECORDS_PROP, 0);
      totalFileSize = PropertyUtil.propertyAsLong(summary, SnapshotSummary.TOTAL_FILE_SIZE_PROP, 0);
      totalFileCount = totalDeleteFiles + totalDataFiles;
      // 计算悬挂删除文件数
      danglingDeleteFileCount =
          totalDeleteFiles - equalityDeleteFileCount - positionalDeleteFileCount;
    }

    /**
     * 初始化方法
     * @param evaluators 分区评估器集合
     */
    private void initialize(Collection<PartitionEvaluator> evaluators) {
      double totalHealthScore = 0;
      for (PartitionEvaluator evaluator : evaluators) {
        addPartitionData(evaluator);
        totalHealthScore += evaluator.getHealthScore();
      }
      // 计算平均健康分数
      healthScore = avgHealthScore(totalHealthScore, evaluators.size());
    }

    /**
     * 添加分区数据到统计
     * @param evaluator 分区评估器
     */
    private void addPartitionData(PartitionEvaluator evaluator) {
      // 添加分区信息
      partitions
          .computeIfAbsent(evaluator.getPartition().first(), ignore -> Sets.newHashSet())
          .add(evaluator.getPartition().second());
      // 累加各种文件统计信息
      dataFileCount += evaluator.getFragmentFileCount() + evaluator.getSegmentFileCount();
      dataFileSize += evaluator.getFragmentFileSize() + evaluator.getSegmentFileSize();
      dataFileRecords += evaluator.getFragmentFileRecords() + evaluator.getSegmentFileRecords();
      positionalDeleteBytes += evaluator.getPosDeleteFileSize();
      positionalDeleteFileRecords += evaluator.getPosDeleteFileRecords();
      positionalDeleteFileCount += evaluator.getPosDeleteFileCount();
      equalityDeleteBytes += evaluator.getEqualityDeleteFileSize();
      equalityDeleteFileRecords += evaluator.getEqualityDeleteFileRecords();
      equalityDeleteFileCount += evaluator.getEqualityDeleteFileCount();
    }

    /**
     * 计算平均健康分数
     * @param totalHealthScore 总分
     * @param partitionCount 分区数
     * @return 平均健康分数
     */
    private int avgHealthScore(double totalHealthScore, int partitionCount) {
      if (partitionCount == 0) {
        return 100;
      }
      return (int) Math.ceil(totalHealthScore / partitionCount);
    }

    public Map<Integer, Set<StructLike>> getPartitions() {
      return partitions;
    }
    // 以下为各种统计信息的getter方法...

    public int getDataFileCount() {
      return dataFileCount;
    }

    public long getDataFileSize() {
      return dataFileSize;
    }

    public long getDataFileRecords() {
      return dataFileRecords;
    }

    public int getEqualityDeleteFileCount() {
      return equalityDeleteFileCount;
    }

    public int getPositionalDeleteFileCount() {
      return positionalDeleteFileCount;
    }

    public long getPositionalDeleteBytes() {
      return positionalDeleteBytes;
    }

    public long getEqualityDeleteBytes() {
      return equalityDeleteBytes;
    }

    public long getEqualityDeleteFileRecords() {
      return equalityDeleteFileRecords;
    }

    public long getPositionalDeleteFileRecords() {
      return positionalDeleteFileRecords;
    }

    public int getHealthScore() {
      return healthScore;
    }

    public int getTotalFileCount() {
      return totalFileCount;
    }

    public long getTotalFileSize() {
      return totalFileSize;
    }

    public long getTotalFileRecords() {
      return totalFileRecords;
    }

    public int getDanglingDeleteFileCount() {
      return danglingDeleteFileCount;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("totalFileCount", totalFileCount)
          .add("totalFileSize", totalFileSize)
          .add("totalFileRecords", totalFileRecords)
          .add("partitions", partitions)
          .add("dataFileCount", dataFileCount)
          .add("dataFileSize", dataFileSize)
          .add("dataFileRecords", dataFileRecords)
          .add("equalityDeleteFileCount", equalityDeleteFileCount)
          .add("positionalDeleteFileCount", positionalDeleteFileCount)
          .add("positionalDeleteBytes", positionalDeleteBytes)
          .add("equalityDeleteBytes", equalityDeleteBytes)
          .add("equalityDeleteFileRecords", equalityDeleteFileRecords)
          .add("positionalDeleteFileRecords", positionalDeleteFileRecords)
          .add("healthScore", healthScore)
          .add("danglingDeleteFileCount", danglingDeleteFileCount)
          .toString();
    }
  }
}
