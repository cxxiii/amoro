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

package org.apache.amoro.server.table;

import static org.apache.amoro.metrics.MetricDefine.defineGauge;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.UnkeyedTable;

/**
 * 表摘要指标类，用于收集和注册表的各种摘要信息指标
 */
public class TableSummaryMetrics extends AbstractTableMetrics {

  // 表摘要文件数量相关指标
  public static final MetricDefine TABLE_SUMMARY_TOTAL_FILES =
      defineGauge("table_summary_total_files")
          .withDescription("表中文件总数")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES =
      defineGauge("table_summary_data_files")
          .withDescription("表中数据文件数量")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES =
      defineGauge("table_summary_equality_delete_files")
          .withDescription("表中等值删除文件数量")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES =
      defineGauge("table_summary_position_delete_files")
          .withDescription("表中位置删除文件数量")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DANGLING_DELETE_FILES =
      defineGauge("table_summary_dangling_delete_files")
          .withDescription("表中悬空删除文件数量")
          .withTags("catalog", "database", "table")
          .build();

  // 表摘要文件大小相关指标
  public static final MetricDefine TABLE_SUMMARY_TOTAL_FILES_SIZE =
      defineGauge("table_summary_total_files_size")
          .withDescription("表中文件总大小")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES_SIZE =
      defineGauge("table_summary_data_files_size")
          .withDescription("表中数据文件大小")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE =
      defineGauge("table_summary_equality_delete_files_size")
          .withDescription("表中等值删除文件大小")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE =
      defineGauge("table_summary_position_delete_files_size")
          .withDescription("表中位置删除文件大小")
          .withTags("catalog", "database", "table")
          .build();

  // 表摘要文件记录数相关指标
  public static final MetricDefine TABLE_SUMMARY_TOTAL_RECORDS =
      defineGauge("table_summary_total_records")
          .withDescription("表中总记录数")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_DATA_FILES_RECORDS =
      defineGauge("table_summary_data_files_records")
          .withDescription("表中数据文件记录数")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS =
      defineGauge("table_summary_equality_delete_files_records")
          .withDescription("表中等值删除文件记录数")
          .withTags("catalog", "database", "table")
          .build();

  public static final MetricDefine TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS =
      defineGauge("table_summary_position_delete_files_records")
          .withDescription("表中位置删除文件记录数")
          .withTags("catalog", "database", "table")
          .build();

  // 表摘要快照数量指标
  public static final MetricDefine TABLE_SUMMARY_SNAPSHOTS =
      defineGauge("table_summary_snapshots")
          .withDescription("表中快照数量")
          .withTags("catalog", "database", "table")
          .build();

  // 表健康评分指标
  public static final MetricDefine TABLE_SUMMARY_HEALTH_SCORE =
      defineGauge("table_summary_health_score")
          .withDescription("表健康评分")
          .withTags("catalog", "database", "table")
          .build();

  // 表摘要数据
  private AbstractOptimizingEvaluator.PendingInput tableSummary =
      new AbstractOptimizingEvaluator.PendingInput();

  // 快照数量
  private long snapshots = 0L;

  /**
   * 构造函数
   * @param identifier 表标识符
   */
  public TableSummaryMetrics(ServerTableIdentifier identifier) {
    super(identifier);
  }

  /**
   * 注册所有指标到指标注册表中
   * @param registry 指标注册表
   */
  @Override
  public void registerMetrics(MetricRegistry registry) {
    if (globalRegistry == null) {
      // 注册文件数量相关指标
      registerMetric(
          registry,
          TABLE_SUMMARY_TOTAL_FILES,
          (Gauge<Long>) () -> (long) tableSummary.getTotalFileCount());
      registerMetric(
          registry,
          TABLE_SUMMARY_DATA_FILES,
          (Gauge<Long>) () -> (long) tableSummary.getDataFileCount());
      registerMetric(
          registry,
          TABLE_SUMMARY_POSITION_DELETE_FILES,
          (Gauge<Long>) () -> (long) tableSummary.getPositionalDeleteFileCount());
      registerMetric(
          registry,
          TABLE_SUMMARY_EQUALITY_DELETE_FILES,
          (Gauge<Long>) () -> (long) tableSummary.getEqualityDeleteFileCount());
      registerMetric(
          registry,
          TABLE_SUMMARY_DANGLING_DELETE_FILES,
          (Gauge<Long>) () -> (long) tableSummary.getDanglingDeleteFileCount());

      // 注册文件大小相关指标
      registerMetric(
          registry,
          TABLE_SUMMARY_TOTAL_FILES_SIZE,
          (Gauge<Long>) () -> tableSummary.getTotalFileSize());
      registerMetric(
          registry,
          TABLE_SUMMARY_DATA_FILES_SIZE,
          (Gauge<Long>) () -> tableSummary.getDataFileSize());
      registerMetric(
          registry,
          TABLE_SUMMARY_POSITION_DELETE_FILES_SIZE,
          (Gauge<Long>) () -> tableSummary.getPositionalDeleteBytes());
      registerMetric(
          registry,
          TABLE_SUMMARY_EQUALITY_DELETE_FILES_SIZE,
          (Gauge<Long>) () -> tableSummary.getEqualityDeleteBytes());

      // 注册文件记录数相关指标
      registerMetric(
          registry,
          TABLE_SUMMARY_TOTAL_RECORDS,
          (Gauge<Long>) () -> tableSummary.getTotalFileRecords());
      registerMetric(
          registry,
          TABLE_SUMMARY_DATA_FILES_RECORDS,
          (Gauge<Long>) () -> tableSummary.getDataFileRecords());
      registerMetric(
          registry,
          TABLE_SUMMARY_POSITION_DELETE_FILES_RECORDS,
          (Gauge<Long>) () -> tableSummary.getPositionalDeleteFileRecords());
      registerMetric(
          registry,
          TABLE_SUMMARY_EQUALITY_DELETE_FILES_RECORDS,
          (Gauge<Long>) () -> tableSummary.getEqualityDeleteFileRecords());

      // 注册健康评分指标
      registerMetric(
          registry,
          TABLE_SUMMARY_HEALTH_SCORE,
          (Gauge<Long>) () -> (long) tableSummary.getHealthScore());

      // 注册快照数量指标
      registerMetric(registry, TABLE_SUMMARY_SNAPSHOTS, (Gauge<Long>) () -> snapshots);

      globalRegistry = registry;
    }
  }

  /**
   * 刷新表摘要数据
   * @param tableSummary 新的表摘要数据
   */
  public void refresh(AbstractOptimizingEvaluator.PendingInput tableSummary) {
    if (tableSummary == null) {
      return;
    }
    this.tableSummary = tableSummary;
  }

  /**
   * 刷新快照数量
   * @param table 混合表对象
   */
  public void refreshSnapshots(MixedTable table) {
    UnkeyedTable unkeyedTable =
        table.isKeyedTable() ? table.asKeyedTable().baseTable() : table.asUnkeyedTable();
    snapshots = Lists.newArrayList(unkeyedTable.snapshots().iterator()).size();
  }
}
