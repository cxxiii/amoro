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
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.utils.TableFileUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * 通用分区评估器，用于评估分区是否需要优化以及优化类型
 */
public class CommonPartitionEvaluator implements PartitionEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(CommonPartitionEvaluator.class);

  // 用于跟踪已处理的删除文件，避免重复计算
  private final Set<String> deleteFileSet = Sets.newHashSet();

  // 分区信息
  private final Pair<Integer, StructLike> partition;
  // 表标识符
  protected final ServerTableIdentifier identifier;
  // 优化配置
  protected final OptimizingConfig config;
  // 上次全量优化时间
  protected final long lastFullOptimizingTime;
  // 上次小优化时间
  protected final long lastMinorOptimizingTime;
  // 碎片文件大小阈值
  protected final long fragmentSize;
  // 最小目标大小
  protected final long minTargetSize;
  // 计划时间
  protected final long planTime;

  // 是否达到全量优化间隔
  private final boolean reachFullInterval;

  // 碎片文件统计
  protected int fragmentFileCount = 0;
  protected long fragmentFileSize = 0;
  protected long fragmentFileRecords = 0;

  // 段文件统计
  protected int rewriteSegmentFileCount = 0;
  protected long rewriteSegmentFileSize = 0L;
  protected long rewriteSegmentFileRecords = 0L;
  protected int undersizedSegmentFileCount = 0;
  protected long undersizedSegmentFileSize = 0;
  protected long undersizedSegmentFileRecords = 0;
  protected int rewritePosSegmentFileCount = 0;
  protected int combinePosSegmentFileCount = 0;
  protected long rewritePosSegmentFileSize = 0L;
  protected long rewritePosSegmentFileRecords = 0L;
  protected long min1SegmentFileSize = Integer.MAX_VALUE;
  protected long min2SegmentFileSize = Integer.MAX_VALUE;

  // 删除文件统计
  protected int equalityDeleteFileCount = 0;
  protected long equalityDeleteFileSize = 0L;
  protected long equalityDeleteFileRecords = 0L;
  protected int posDeleteFileCount = 0;
  protected long posDeleteFileSize = 0L;
  protected long posDeleteFileRecords = 0L;

  // 缓存的计算结果
  private long cost = -1;
  private Boolean necessary = null;
  private OptimizingType optimizingType = null;
  private String name;

  /**
   * 构造函数
   * @param identifier 表标识符
   * @param config 优化配置
   * @param partition 分区信息
   * @param planTime 计划时间
   * @param lastMinorOptimizingTime 上次小优化时间
   * @param lastFullOptimizingTime 上次全量优化时间
   */
  public CommonPartitionEvaluator(
      ServerTableIdentifier identifier,
      OptimizingConfig config,
      Pair<Integer, StructLike> partition,
      long planTime,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime) {
    this.identifier = identifier;
    this.config = config;
    this.partition = partition;
    this.fragmentSize = config.getTargetSize() / config.getFragmentRatio();
    this.minTargetSize = (long) (config.getTargetSize() * config.getMinTargetSizeRatio());
    if (minTargetSize > config.getTargetSize() - fragmentSize) {
      LOG.warn(
          "The self-optimizing.min-target-size-ratio is set too large, some segment files will not be able to find "
              + "the another merge file.");
    }
    this.planTime = planTime;
    this.lastMinorOptimizingTime = lastMinorOptimizingTime;
    this.lastFullOptimizingTime = lastFullOptimizingTime;
    this.reachFullInterval =
        config.getFullTriggerInterval() >= 0
            && planTime - lastFullOptimizingTime > config.getFullTriggerInterval();
  }

  @Override
  public Pair<Integer, StructLike> getPartition() {
    return partition;
  }

  /**
   * 判断是否为碎片文件
   * @param dataFile 数据文件
   * @return 是否为碎片文件
   */
  protected boolean isFragmentFile(DataFile dataFile) {
    return dataFile.fileSizeInBytes() <= fragmentSize;
  }

  /**
   * 判断是否为小于目标大小的段文件
   * @param dataFile 数据文件
   * @return 是否为小于目标大小的段文件
   */
  protected boolean isUndersizedSegmentFile(DataFile dataFile) {
    return dataFile.fileSizeInBytes() > fragmentSize && dataFile.fileSizeInBytes() <= minTargetSize;
  }

  /**
   * 添加文件到评估器
   * @param dataFile 数据文件
   * @param deletes 关联的删除文件
   * @return 是否成功添加
   */
  @Override
  public boolean addFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (!config.isEnabled()) {
      return false;
    }
    if (isFragmentFile(dataFile)) {
      return addFragmentFile(dataFile, deletes);
    } else if (isUndersizedSegmentFile(dataFile)) {
      return addUndersizedSegmentFile(dataFile, deletes);
    } else {
      return addTargetSizeReachedFile(dataFile, deletes);
    }
  }

  /**
   * 检查是否为重复的删除文件
   * @param delete 删除文件
   * @return 是否已存在
   */
  private boolean isDuplicateDelete(ContentFile<?> delete) {
    boolean deleteExist = deleteFileSet.contains(delete.path().toString());
    if (!deleteExist) {
      deleteFileSet.add(delete.path().toString());
    }
    return deleteExist;
  }

  /**
   * 添加碎片文件
   * @param dataFile 数据文件
   * @param deletes 关联的删除文件
   * @return 是否成功添加
   */
  private boolean addFragmentFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    fragmentFileSize += dataFile.fileSizeInBytes();
    fragmentFileCount++;
    fragmentFileRecords += dataFile.recordCount();

    for (ContentFile<?> delete : deletes) {
      addDelete(delete);
    }
    return true;
  }

  /**
   * 添加小于目标大小的段文件
   * @param dataFile 数据文件
   * @param deletes 关联的删除文件
   * @return 是否成功添加
   */
  private boolean addUndersizedSegmentFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    // 因为UndersizedSegment可以在拆分任务阶段确定是否重写
    // 所以计算的posDeleteFileCount、posDeleteFileSize、equalityDeleteFileCount、
    // equalityDeleteFileSize不准确
    for (ContentFile<?> delete : deletes) {
      addDelete(delete);
    }
    if (fileShouldRewrite(dataFile, deletes)) {
      rewriteSegmentFileSize += dataFile.fileSizeInBytes();
      rewriteSegmentFileCount++;
      rewriteSegmentFileRecords += dataFile.recordCount();
      return true;
    }

    // 缓存最小的两个文件的大小
    if (dataFile.fileSizeInBytes() < min1SegmentFileSize) {
      min2SegmentFileSize = min1SegmentFileSize;
      min1SegmentFileSize = dataFile.fileSizeInBytes();
    } else if (dataFile.fileSizeInBytes() < min2SegmentFileSize) {
      min2SegmentFileSize = dataFile.fileSizeInBytes();
    }

    undersizedSegmentFileSize += dataFile.fileSizeInBytes();
    undersizedSegmentFileCount++;
    undersizedSegmentFileRecords += dataFile.recordCount();
    return true;
  }

  /**
   * 添加已达到目标大小的文件
   * @param dataFile 数据文件
   * @param deletes 关联的删除文件
   * @return 是否成功添加
   */
  private boolean addTargetSizeReachedFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (fileShouldRewrite(dataFile, deletes)) {
      rewriteSegmentFileSize += dataFile.fileSizeInBytes();
      rewriteSegmentFileCount++;
      rewriteSegmentFileRecords += dataFile.recordCount();
      for (ContentFile<?> delete : deletes) {
        addDelete(delete);
      }
      return true;
    }

    if (segmentShouldRewritePos(dataFile, deletes)) {
      rewritePosSegmentFileSize += dataFile.fileSizeInBytes();
      rewritePosSegmentFileCount++;
      rewritePosSegmentFileRecords += dataFile.recordCount();
      for (ContentFile<?> delete : deletes) {
        addDelete(delete);
      }
      return true;
    }

    return false;
  }

  /**
   * 判断文件是否需要全量优化
   * @param dataFile 数据文件
   * @param deleteFiles 删除文件列表
   * @return 是否需要全量优化
   */
  protected boolean fileShouldFullOptimizing(DataFile dataFile, List<ContentFile<?>> deleteFiles) {
    if (config.isFullRewriteAllFiles()) {
      return true;
    }
    // 如果文件关联了任何删除文件或者不够大，应该进行全量优化
    return !deleteFiles.isEmpty() || isFragmentFile(dataFile) || isUndersizedSegmentFile(dataFile);
  }

  /**
   * 判断文件是否需要重写
   * @param dataFile 数据文件
   * @param deletes 删除文件列表
   * @return 是否需要重写
   */
  public boolean fileShouldRewrite(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (isFullOptimizing()) {
      return fileShouldFullOptimizing(dataFile, deletes);
    }
    if (isFragmentFile(dataFile)) {
      return true;
    }
    // 当在Flink引擎中启用Upsert写入时，INSERT和UPDATE_AFTER都会生成删除文件（主要是eq-delete）
    // eq-delete文件将与当前快照之前的数据文件关联
    // eq-delete不能准确反映当前段文件中删除了多少数据（即是否需要重写段文件）
    // 并且eq-delete文件在小优化期间会转换为pos-delete，所以这里只计算pos-delete记录数
    return getPosDeletesRecordCount(deletes)
        > dataFile.recordCount() * config.getMajorDuplicateRatio();
  }

  /**
   * 判断段文件是否需要重写位置信息
   * @param dataFile 数据文件
   * @param deletes 删除文件列表
   * @return 是否需要重写位置信息
   */
  public boolean segmentShouldRewritePos(DataFile dataFile, List<ContentFile<?>> deletes) {
    Preconditions.checkArgument(!isFragmentFile(dataFile), "Unsupported fragment file.");
    long equalDeleteFileCount = 0;
    long posDeleteFileCount = 0;

    for (ContentFile<?> delete : deletes) {
      if (delete.content() == FileContent.EQUALITY_DELETES) {
        equalDeleteFileCount++;
      } else if (delete.content() == FileContent.POSITION_DELETES) {
        posDeleteFileCount++;
      }
    }
    if (posDeleteFileCount > 1) {
      combinePosSegmentFileCount++;
      return true;
    } else if (equalDeleteFileCount > 0) {
      return true;
    } else if (posDeleteFileCount == 1) {
      return !TableFileUtil.isOptimizingPosDeleteFile(
          dataFile.path().toString(), deletes.get(0).path().toString());
    } else {
      return false;
    }
  }

  /**
   * 是否处于全量优化状态
   * @return 是否处于全量优化状态
   */
  protected boolean isFullOptimizing() {
    return reachFullInterval();
  }

  /**
   * 获取位置删除记录数
   * @param files 文件列表
   * @return 位置删除记录数
   */
  private long getPosDeletesRecordCount(List<ContentFile<?>> files) {
    return files.stream()
        .filter(file -> file.content() == FileContent.POSITION_DELETES)
        .mapToLong(ContentFile::recordCount)
        .sum();
  }

  /**
   * 添加删除文件
   * @param delete 删除文件
   */
  private void addDelete(ContentFile<?> delete) {
    if (isDuplicateDelete(delete)) {
      return;
    }
    if (delete.content() == FileContent.POSITION_DELETES) {
      posDeleteFileCount++;
      posDeleteFileSize += delete.fileSizeInBytes();
      posDeleteFileRecords += delete.recordCount();
    } else {
      equalityDeleteFileCount++;
      equalityDeleteFileSize += delete.fileSizeInBytes();
      equalityDeleteFileRecords += delete.recordCount();
    }
  }

  @Override
  public boolean isNecessary() {
    if (necessary == null) {
      if (isFullOptimizing()) {
        necessary = isFullNecessary();
      } else {
        necessary = isMajorNecessary() || isMinorNecessary();
      }
      LOG.debug("{} necessary = {}, {}", name(), necessary, this);
    }
    return necessary;
  }

  @Override
  public long getCost() {
    if (cost < 0) {
      // 我们估计写入成本与读取成本相同
      // 当重写位置删除文件时，只会读取段文件的主键字段，所以只计算大小的十分之一
      cost =
          (fragmentFileSize + rewriteSegmentFileSize + undersizedSegmentFileSize) * 2
              + rewritePosSegmentFileSize / 10
              + posDeleteFileSize
              + equalityDeleteFileSize;
      int fileCnt =
          fragmentFileCount
              + rewriteSegmentFileCount
              + undersizedSegmentFileCount
              + rewritePosSegmentFileCount
              + posDeleteFileCount
              + equalityDeleteFileCount;
      cost += fileCnt * config.getOpenFileCost();
    }
    return cost;
  }

  @Override
  public PartitionEvaluator.Weight getWeight() {
    return new Weight(getCost());
  }

  @Override
  public OptimizingType getOptimizingType() {
    if (optimizingType == null) {
      optimizingType =
          isFullNecessary()
              ? OptimizingType.FULL
              : isMajorNecessary() ? OptimizingType.MAJOR : OptimizingType.MINOR;
      LOG.debug("{} optimizingType = {} ", name(), optimizingType);
    }
    return optimizingType;
  }

  /**
   * 段文件是否有足够的内容
   *
   * <p>1. 所有小于目标大小的段文件总大小大于目标大小
   *
   * <p>2. 有两个小于目标大小的段文件可以合并为一个
   */
  public boolean enoughContent() {
    return undersizedSegmentFileSize >= config.getTargetSize()
        && min1SegmentFileSize + min2SegmentFileSize <= config.getTargetSize();
  }

  /**
   * 是否需要主优化
   * @return 是否需要主优化
   */
  public boolean isMajorNecessary() {
    return enoughContent() || rewriteSegmentFileCount > 0;
  }

  /**
   * 是否需要小优化
   * @return 是否需要小优化
   */
  public boolean isMinorNecessary() {
    int smallFileCount = fragmentFileCount + equalityDeleteFileCount;
    return smallFileCount >= config.getMinorLeastFileCount()
        || (smallFileCount > 1 && reachMinorInterval())
        || combinePosSegmentFileCount > 0;
  }

  /**
   * 是否达到小优化间隔
   * @return 是否达到小优化间隔
   */
  protected boolean reachMinorInterval() {
    return config.getMinorLeastInterval() >= 0
        && planTime - lastMinorOptimizingTime > config.getMinorLeastInterval();
  }

  /**
   * 是否达到全量优化间隔
   * @return 是否达到全量优化间隔
   */
  protected boolean reachFullInterval() {
    return reachFullInterval;
  }

  /**
   * 是否需要全量优化
   * @return 是否需要全量优化
   */
  public boolean isFullNecessary() {
    if (!reachFullInterval()) {
      return false;
    }
    return anyDeleteExist()
        || fragmentFileCount >= 2
        || undersizedSegmentFileCount >= 2
        || rewriteSegmentFileCount > 0
        || rewritePosSegmentFileCount > 0;
  }

  /**
   * 获取分区名称
   * @return 分区名称
   */
  protected String name() {
    if (name == null) {
      name = String.format("partition %s of %s", partition, identifier.toString());
    }
    return name;
  }

  /**
   * 是否存在任何删除文件
   * @return 是否存在删除文件
   */
  public boolean anyDeleteExist() {
    return equalityDeleteFileCount > 0 || posDeleteFileCount > 0;
  }

  @Override
  public int getHealthScore() {
    long dataFilesSize = getFragmentFileSize() + getSegmentFileSize();
    long dataFiles = getFragmentFileCount() + getSegmentFileCount();
    long dataRecords = getFragmentFileRecords() + getSegmentFileRecords();

    double averageDataFileSize = getNormalizedRatio(dataFilesSize, dataFiles);
    double eqDeleteRatio = getNormalizedRatio(equalityDeleteFileRecords, dataRecords);
    double posDeleteRatio = getNormalizedRatio(posDeleteFileRecords, dataRecords);

    double tablePenaltyFactor = getTablePenaltyFactor(dataFiles, dataFilesSize);
    return (int)
        Math.ceil(
            100
                - tablePenaltyFactor
                    * (40 * getSmallFilePenaltyFactor(averageDataFileSize)
                        + 40 * getEqDeletePenaltyFactor(eqDeleteRatio)
                        + 20 * getPosDeletePenaltyFactor(posDeleteRatio)));
  }

  /**
   * 获取相等删除惩罚因子
   * @param eqDeleteRatio 相等删除比率
   * @return 惩罚因子
   */
  private double getEqDeletePenaltyFactor(double eqDeleteRatio) {
    double eqDeleteRatioThreshold = config.getMajorDuplicateRatio();
    return getNormalizedRatio(eqDeleteRatio, eqDeleteRatioThreshold);
  }

  /**
   * 获取位置删除惩罚因子
   * @param posDeleteRatio 位置删除比率
   * @return 惩罚因子
   */
  private double getPosDeletePenaltyFactor(double posDeleteRatio) {
    double posDeleteRatioThreshold = config.getMajorDuplicateRatio() * 2;
    return getNormalizedRatio(posDeleteRatio, posDeleteRatioThreshold);
  }

  /**
   * 获取小文件惩罚因子
   * @param averageDataFileSize 平均数据文件大小
   * @return 惩罚因子
   */
  private double getSmallFilePenaltyFactor(double averageDataFileSize) {
    return 1 - getNormalizedRatio(averageDataFileSize, minTargetSize);
  }

  /**
   * 获取表惩罚因子
   * @param dataFiles 数据文件数量
   * @param dataFilesSize 数据文件总大小
   * @return 惩罚因子
   */
  private double getTablePenaltyFactor(long dataFiles, long dataFilesSize) {
    // 如果表文件数量小于等于1，没有惩罚，即表被认为是完全健康的
    if (dataFiles <= 1) {
      return 0;
    }
    // 小表对性能影响很小，所以只有很小的惩罚
    return getNormalizedRatio(dataFiles, config.getMinorLeastFileCount())
        * getNormalizedRatio(dataFilesSize, config.getTargetSize());
  }

  /**
   * 获取标准化比率
   * @param numerator 分子
   * @param denominator 分母
   * @return 标准化比率
   */
  private double getNormalizedRatio(double numerator, double denominator) {
    if (denominator <= 0) {
      return 0;
    }
    return Math.min(numerator, denominator) / denominator;
  }

  @Override
  public int getFragmentFileCount() {
    return fragmentFileCount;
  }

  @Override
  public long getFragmentFileSize() {
    return fragmentFileSize;
  }

  @Override
  public long getFragmentFileRecords() {
    return fragmentFileRecords;
  }

  @Override
  public int getSegmentFileCount() {
    return rewriteSegmentFileCount + undersizedSegmentFileCount + rewritePosSegmentFileCount;
  }

  @Override
  public long getSegmentFileSize() {
    return rewriteSegmentFileSize + undersizedSegmentFileSize + rewritePosSegmentFileSize;
  }

  @Override
  public long getSegmentFileRecords() {
    return rewriteSegmentFileRecords + undersizedSegmentFileRecords + rewritePosSegmentFileRecords;
  }

  @Override
  public int getEqualityDeleteFileCount() {
    return equalityDeleteFileCount;
  }

  @Override
  public long getEqualityDeleteFileSize() {
    return equalityDeleteFileSize;
  }

  @Override
  public long getEqualityDeleteFileRecords() {
    return equalityDeleteFileRecords;
  }

  @Override
  public int getPosDeleteFileCount() {
    return posDeleteFileCount;
  }

  @Override
  public long getPosDeleteFileSize() {
    return posDeleteFileSize;
  }

  @Override
  public long getPosDeleteFileRecords() {
    return posDeleteFileRecords;
  }

  /**
   * 权重类，用于比较分区优化成本
   */
  public static class Weight implements PartitionEvaluator.Weight {

    private final long cost;

    public Weight(long cost) {
      this.cost = cost;
    }

    @Override
    public int compareTo(PartitionEvaluator.Weight o) {
      return Long.compare(this.cost, ((Weight) o).cost);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partition", partition)
        .add("config", config)
        .add("fragmentSize", fragmentSize)
        .add("undersizedSegmentSize", minTargetSize)
        .add("planTime", planTime)
        .add("lastMinorOptimizeTime", lastMinorOptimizingTime)
        .add("lastFullOptimizeTime", lastFullOptimizingTime)
        .add("fragmentFileCount", fragmentFileCount)
        .add("fragmentFileSize", fragmentFileSize)
        .add("fragmentFileRecords", fragmentFileRecords)
        .add("rewriteSegmentFileCount", rewriteSegmentFileCount)
        .add("rewriteSegmentFileSize", rewriteSegmentFileSize)
        .add("rewriteSegmentFileRecords", rewriteSegmentFileRecords)
        .add("undersizedSegmentFileCount", undersizedSegmentFileCount)
        .add("undersizedSegmentFileSize", undersizedSegmentFileSize)
        .add("undersizedSegmentFileRecords", undersizedSegmentFileRecords)
        .add("rewritePosSegmentFileCount", rewritePosSegmentFileCount)
        .add("rewritePosSegmentFileSize", rewritePosSegmentFileSize)
        .add("rewritePosSegmentFileRecords", rewritePosSegmentFileRecords)
        .add("min1SegmentFileSize", min1SegmentFileSize)
        .add("min2SegmentFileSize", min2SegmentFileSize)
        .add("equalityDeleteFileCount", equalityDeleteFileCount)
        .add("equalityDeleteFileSize", equalityDeleteFileSize)
        .add("equalityDeleteFileRecords", equalityDeleteFileRecords)
        .add("posDeleteFileCount", posDeleteFileCount)
        .add("posDeleteFileSize", posDeleteFileSize)
        .add("posDeleteFileRecords", posDeleteFileRecords)
        .toString();
  }
}
