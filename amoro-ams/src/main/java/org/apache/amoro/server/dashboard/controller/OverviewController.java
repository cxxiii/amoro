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

import io.javalin.http.Context;
import org.apache.amoro.server.dashboard.OverviewManager;
import org.apache.amoro.server.dashboard.model.OverviewDataSizeItem;
import org.apache.amoro.server.dashboard.model.OverviewResourceUsageItem;
import org.apache.amoro.server.dashboard.model.OverviewSummary;
import org.apache.amoro.server.dashboard.model.OverviewTopTableItem;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 处理概览页面请求的控制器
 */
public class OverviewController {

  private final OverviewManager manager;

  /**
   * 构造函数
   * @param manager 概览管理服务实例
   */
  public OverviewController(OverviewManager manager) {
    this.manager = manager;
  }

  /**
   * 获取资源使用历史数据
   * @param ctx HTTP请求上下文，包含请求参数和响应方法
   * 方法流程：
   * 1. 从请求参数中获取startTime
   * 2. 校验startTime是否为有效数字
   * 3. 调用manager获取从指定时间开始的资源使用历史数据
   * 4. 将结果包装成OkResponse格式返回
   */
  public void getResourceUsageHistory(Context ctx) {
    // 从请求参数中获取startTime
    String startTime = ctx.queryParam("startTime");
    // 校验startTime是否为有效数字，如果不是则抛出异常
    Preconditions.checkArgument(StringUtils.isNumeric(startTime), "invalid startTime!");
    // 调用manager获取资源使用历史数据
    List<OverviewResourceUsageItem> resourceUsageHistory =
        manager.getResourceUsageHistory(Long.parseLong(startTime));
    // 将结果包装成统一响应格式返回
    ctx.json(OkResponse.of(resourceUsageHistory));
  }

  /**
   * 获取数据大小历史记录
   * @param ctx HTTP请求上下文
   */
  public void getDataSizeHistory(Context ctx) {
    String startTime = ctx.queryParam("startTime");
    Preconditions.checkArgument(StringUtils.isNumeric(startTime), "invalid startTime!");
    List<OverviewDataSizeItem> dataSizeHistory =
        manager.getDataSizeHistory(Long.parseLong(startTime));
    //   OkResponse.of() 是一个静态工厂方法，用于创建统一格式的成功响应对象。它将原始数据包装成如下结构：
    //  {  "status": "success",  "data": [资源使用历史数据列表]}
    ctx.json(OkResponse.of(dataSizeHistory));
  }

/**
 * 获取排名靠前的表数据
 *
 * @param ctx HTTP请求上下文，包含请求参数和响应方法
 * 方法流程：
 * 1. 从请求参数中获取排序方式(order)、排序字段(orderBy)和结果数量限制(limit)
 * 2. 校验参数有效性
 * 3. 根据排序字段创建不同的比较器
 * 4. 调用getTopTables方法获取排序后的表数据
 * 5. 将结果包装成OkResponse格式返回
 */
public void getTopTables(Context ctx) {
    String order = ctx.queryParam("order");  // 排序方式(asc/desc)
    String orderBy = ctx.queryParam("orderBy");  // 排序字段
    Integer limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);  // 返回结果数量限制，默认10条
    // 校验参数不能为空
    Preconditions.checkArgument(StringUtils.isNotBlank(order), "order can not be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(orderBy), "orderBy can not be empty");

    List<OverviewTopTableItem> top10Tables;
    boolean isAsc = "asc".equals(order);  // 是否升序排序

    // 根据排序字段选择不同的比较器
    switch (orderBy) {
      case "tableSize":
        // 按表大小排序
        // 创建Comparator比较器，用于对OverviewTopTableItem对象进行排序
        // 使用OverviewTopTableItem类的getTableSize()方法返回的值作为比较依据
        Comparator<OverviewTopTableItem> tableSizeComparator =
            Comparator.comparingLong(OverviewTopTableItem::getTableSize);
        top10Tables = getTopTables(isAsc, tableSizeComparator, limit);
        break;
      case "fileCount":
        // 按文件数量排序
        Comparator<OverviewTopTableItem> fileCountComparator =
            Comparator.comparingLong(OverviewTopTableItem::getFileCount);
        top10Tables = getTopTables(isAsc, fileCountComparator, limit);
        break;
      case "healthScore":
        // 按健康分数排序，未计算的健康分数(-1)排在列表末尾
        Comparator<OverviewTopTableItem> healthScoreComparator =
            Comparator.comparingLong(
                item ->
                    item.getHealthScore() < 0
                        ? (isAsc ? Long.MAX_VALUE : Long.MIN_VALUE)  // 未计算分数的特殊处理
                        : item.getTableSize()); //todo 这里的getTableSize()方法应该替换为getHealthScore()方法
        top10Tables = getTopTables(isAsc, healthScoreComparator, limit);
        break;
      default:
        throw new IllegalArgumentException("Invalid orderBy: " + orderBy);
    }
    // 返回JSON格式的响应
    ctx.json(OkResponse.of(top10Tables));
  }

  /**
   * 获取排序后的表列表
   *
   * @param asc 是否升序排序，true表示升序，false表示降序
   * @param comparator 用于排序的比较器，定义了表项的排序规则
   * @param limit 返回结果的数量限制
   * @return 排序后的表列表，数量不超过limit参数指定的值
   *
   * 方法流程：
   * 1. 从manager获取所有表项数据
   * 2. 根据asc参数决定排序方向：
   *    - 升序：使用原始比较器后接表名比较
   *    - 降序：使用反转后的比较器后接表名比较
   * 3. 限制结果数量
   * 4. 收集结果并返回
   */
  private List<OverviewTopTableItem> getTopTables(
      boolean asc, Comparator<OverviewTopTableItem> comparator, int limit) {
    return manager.getAllTopTableItem().stream()
        .sorted(
            asc
                ? comparator.thenComparing(OverviewTopTableItem::getTableName)  // 升序排序：先按比较器排序，再按表名排序
                : comparator.reversed().thenComparing(OverviewTopTableItem::getTableName))  // 降序排序：先反转比较器排序，再按表名排序
        .limit(limit)  // 限制返回结果数量
        .collect(Collectors.toList());  // 收集结果到List
  }

  /**
   * 获取系统概览摘要信息
   * @param ctx HTTP请求上下文
   */
  public void getSummary(Context ctx) {
    int totalCatalog = manager.getTotalCatalog();  // 总目录数
    int totalTableCount = manager.getTotalTableCount();  // 总表数
    long totalDataSize = manager.getTotalDataSize();  // 总数据大小
    int totalCpu = manager.getTotalCpu();  // 总CPU数
    long totalMemory = manager.getTotalMemory();  // 总内存大小

    OverviewSummary overviewSummary =
        new OverviewSummary(totalCatalog, totalTableCount, totalDataSize, totalCpu, totalMemory);
    ctx.json(OkResponse.of(overviewSummary));
  }

  /**
   * 获取优化状态信息
   * @param ctx HTTP请求上下文
   */
  public void getOptimizingStatus(Context ctx) {
    Map<String, Long> optimizingStatus = manager.getOptimizingStatus();
    List<ImmutableMap<String, ? extends Serializable>> optimizingStatusList =
        optimizingStatus.entrySet().stream()
            .map(status -> ImmutableMap.of("name", status.getKey(), "value", status.getValue()))
            .collect(Collectors.toList());
    ctx.json(OkResponse.of(optimizingStatusList));
  }
}
