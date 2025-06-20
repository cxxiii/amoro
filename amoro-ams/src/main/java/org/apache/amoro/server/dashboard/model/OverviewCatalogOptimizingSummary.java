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

public class OverviewCatalogOptimizingSummary {

  private long optimizingProcessCount;
  private long optimizingInputFileCount;
  private long optimizingInputDataSize;
  private long inputFileAverageSize;
  private long outputFileAverageSize;
  private long ts;

  public OverviewCatalogOptimizingSummary() {}

  public OverviewCatalogOptimizingSummary(
      long ts,
      long optimizingProcessCount,
      long optimizingInputFileCount,
      long optimizingInputDataSize,
      long inputFileAverageSize,
      long outputFileAverageSize) {
    this.ts = ts;
    this.optimizingProcessCount = optimizingProcessCount;
    this.optimizingInputFileCount = optimizingInputFileCount;
    this.optimizingInputDataSize = optimizingInputDataSize;
    this.inputFileAverageSize = inputFileAverageSize;
    this.outputFileAverageSize = outputFileAverageSize;
  }

  public OverviewCatalogOptimizingSummary(
      long optimizingProcessCount,
      long optimizingInputFileCount,
      long optimizingInputDataSize,
      long inputFileAverageSize,
      long outputFileAverageSize) {
    this.optimizingProcessCount = optimizingProcessCount;
    this.optimizingInputFileCount = optimizingInputFileCount;
    this.optimizingInputDataSize = optimizingInputDataSize;
    this.inputFileAverageSize = inputFileAverageSize;
    this.outputFileAverageSize = outputFileAverageSize;
  }

  public long getTs() {
    return ts;
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

  public long getInputFileAverageSize() {
    return inputFileAverageSize;
  }

  public void setInputFileAverageSize(long inputFileAverageSize) {
    this.inputFileAverageSize = inputFileAverageSize;
  }

  public long getOutputFileAverageSize() {
    return outputFileAverageSize;
  }

  public void setOutputFileAverageSize(long outPutAverageFileSize) {
    this.outputFileAverageSize = outPutAverageFileSize;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("optimizingProcessCount", optimizingProcessCount)
        .add("optimizingInputFileCount", optimizingInputFileCount)
        .add("optimizingInputDataSize", optimizingInputDataSize)
        .add("inputFileAverageSize", inputFileAverageSize)
        .add("outputFileAverageSize", outputFileAverageSize)
        .toString();
  }
}
