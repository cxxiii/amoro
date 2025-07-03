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

import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;

public class OverviewSummary {

  private int catalogCnt;
  private int tableCnt;
  private long tableTotalSize;
  private int totalCpu;
  private long totalMemory;
  private long optimizingProcessCount;
  private long optimizingInputFileCount;
  private long optimizingInputDataSize;
  private long optimizingOutputFileCount;
  private long optimizingOutputDataSize;

  public OverviewSummary() {}

  public OverviewSummary(
      int catalogCnt, int tableCnt, long tableTotalSize, int totalCpu, long totalMemory) {
    this.catalogCnt = catalogCnt;
    this.tableCnt = tableCnt;
    this.tableTotalSize = tableTotalSize;
    this.totalCpu = totalCpu;
    this.totalMemory = totalMemory;
  }

  public OverviewSummary(
      int catalogCnt,
      int tableCnt,
      long tableTotalSize,
      int totalCpu,
      long totalMemory,
      long optimizingProcessCount,
      long optimizingInputFileCount,
      long optimizingInputDataSize,
      long optimizingOutputFileCount,
      long optimizingOutputDataSize) {
    this.catalogCnt = catalogCnt;
    this.tableCnt = tableCnt;
    this.tableTotalSize = tableTotalSize;
    this.totalCpu = totalCpu;
    this.totalMemory = totalMemory;
    this.optimizingProcessCount = optimizingProcessCount;
    this.optimizingInputFileCount = optimizingInputFileCount;
    this.optimizingInputDataSize = optimizingInputDataSize;
    this.optimizingOutputFileCount = optimizingOutputFileCount;
    this.optimizingOutputDataSize = optimizingOutputDataSize;
  }

  public long getOptimizingProcessCount() {
    return optimizingProcessCount;
  }

  public void setOptimizingProcessCount(long optimizingProcessCount) {
    this.optimizingProcessCount = optimizingProcessCount;
  }

  public long getOptimizingInputFileCount() {
    return optimizingInputFileCount;
  }

  public void setOptimizingInputFileCount(long optimizingInputFileCount) {
    this.optimizingInputFileCount = optimizingInputFileCount;
  }

  public long getOptimizingInputDataSize() {
    return optimizingInputDataSize;
  }

  public void setOptimizingInputDataSize(long optimizingInputDataSize) {
    this.optimizingInputDataSize = optimizingInputDataSize;
  }

  public long getOptimizingOutputFileCount() {
    return optimizingOutputFileCount;
  }

  public void setOptimizingOutputFileCount(long optimizingOutputFileCount) {
    this.optimizingOutputFileCount = optimizingOutputFileCount;
  }

  public long getOptimizingOutputDataSize() {
    return optimizingOutputDataSize;
  }

  public void setOptimizingOutputDataSize(long optimizingOutputDataSize) {
    this.optimizingOutputDataSize = optimizingOutputDataSize;
  }

  public int getCatalogCnt() {
    return catalogCnt;
  }

  public void setCatalogCnt(int catalogCnt) {
    this.catalogCnt = catalogCnt;
  }

  public int getTableCnt() {
    return tableCnt;
  }

  public void setTableCnt(int tableCnt) {
    this.tableCnt = tableCnt;
  }

  public long getTableTotalSize() {
    return tableTotalSize;
  }

  public void setTableTotalSize(long tableTotalSize) {
    this.tableTotalSize = tableTotalSize;
  }

  public int getTotalCpu() {
    return totalCpu;
  }

  public void setTotalCpu(int totalCpu) {
    this.totalCpu = totalCpu;
  }

  public long getTotalMemory() {
    return totalMemory;
  }

  public void setTotalMemory(long totalMemory) {
    this.totalMemory = totalMemory;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("catalogCnt", catalogCnt)
        .add("tableCnt", tableCnt)
        .add("tableTotalSize", tableTotalSize)
        .add("totalCpu", totalCpu)
        .add("totalMemory", totalMemory)
        .add("optimizingProcessCount", optimizingProcessCount)
        .add("optimizingInputFileCount", optimizingInputFileCount)
        .add("optimizingInputDataSize", optimizingInputDataSize)
        .add("inputFileAverageSize", optimizingOutputFileCount)
        .add("outputFileAverageSize", optimizingOutputDataSize)
        .toString();
  }
}
