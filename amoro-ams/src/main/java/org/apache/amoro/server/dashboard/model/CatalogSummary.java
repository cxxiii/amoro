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

package org.apache.amoro.server.dashboard.model;

import com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class CatalogSummary {
  private int tableCount;
  private long tableTotalSize;
  private int totalCpu;
  private long totalMemory;
  private final Map<LocalDateTime, OptimizingSummary> optimizingSummariesPerHour =
      Maps.newConcurrentMap();

  public CatalogSummary() {}

  public void setTableCnt(int tableCnt) {
    this.tableCount = tableCnt;
  }

  public void setTableTotalSize(long tableTotalSize) {
    this.tableTotalSize = tableTotalSize;
  }

  public void setTotalCpu(int totalCpu) {
    this.totalCpu = totalCpu;
  }

  public void setTotalMemory(long totalMemory) {
    this.totalMemory = totalMemory;
  }

  public long getTableTotalSize() {
    return tableTotalSize;
  }

  public OverviewSummary summary(long startTime) {
    LocalDateTime now = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);
    LocalDateTime start =
        Instant.ofEpochMilli(startTime)
            .atZone(TimeZone.getDefault().toZoneId())
            .toLocalDateTime()
            .withMinute(0)
            .withSecond(0)
            .withNano(0);
    List<OptimizingSummary> optimizingSummaries = Lists.newArrayList();
    while (start.isBefore(now) || start.equals(now)) {
      if (optimizingSummariesPerHour.containsKey(start)) {
        optimizingSummaries.add(optimizingSummariesPerHour.get(start));
      }
      start = start.plusHours(1);
    }
    OverviewSummary summary =
        new OverviewSummary(1, tableCount, tableTotalSize, totalCpu, totalMemory);
    summary.setOptimizingProcessCount(
        optimizingSummaries.stream().mapToLong(OptimizingSummary::getOptimizingProcessCount).sum());
    summary.setOptimizingInputDataSize(
        optimizingSummaries.stream()
            .mapToLong(OptimizingSummary::getOptimizingInputDataSize)
            .sum());
    summary.setOptimizingInputFileCount(
        optimizingSummaries.stream()
            .mapToLong(OptimizingSummary::getOptimizingInputFileCount)
            .sum());
    summary.setOptimizingOutputDataSize(
        optimizingSummaries.stream()
            .mapToLong(OptimizingSummary::getOptimizingOutputDataSize)
            .sum());
    summary.setOptimizingOutputFileCount(
        optimizingSummaries.stream()
            .mapToLong(OptimizingSummary::getOptimizingOutputFileCount)
            .sum());
    return summary;
  }

  public Map<LocalDateTime, OptimizingSummary> getOptimizingSummaries() {
    return optimizingSummariesPerHour;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableCount", tableCount)
        .add("tableTotalSize", tableTotalSize)
        .add("totalCpu", totalCpu)
        .add("totalMemory", totalMemory)
        .add("optimizingSummariesPerHour", optimizingSummariesPerHour)
        .toString();
  }
}
