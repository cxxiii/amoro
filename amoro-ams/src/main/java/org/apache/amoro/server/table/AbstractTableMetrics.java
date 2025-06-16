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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.server.metrics.MetricRegistry;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;

import java.util.List;

/**
 * 抽象表指标基类，用于管理和注册表级别的监控指标
 */
public abstract class AbstractTableMetrics {
  // 表标识符
  protected final ServerTableIdentifier identifier;
  // 已注册的指标键列表
  protected final List<MetricKey> registeredMetricKeys = Lists.newArrayList();
  // 全局指标注册器
  protected MetricRegistry globalRegistry;

  /**
   * 构造函数
   * @param identifier 表标识符
   */
  protected AbstractTableMetrics(ServerTableIdentifier identifier) {
    this.identifier = identifier;
  }

  /**
   * 注册单个指标
   * @param registry 指标注册器
   * @param define 指标定义
   * @param metric 指标实例
   */
  protected void registerMetric(MetricRegistry registry, MetricDefine define, Metric metric) {
    MetricKey key =
        registry.register(
            define,
            ImmutableMap.of(
                "catalog",
                identifier.getCatalog(),
                "database",
                identifier.getDatabase(),
                "table",
                identifier.getTableName()),
            metric);
    registeredMetricKeys.add(key);
  }

  /**
   * 注册所有指标到指定的注册器
   * @param registry 指标注册器
   */
  public void register(MetricRegistry registry) {
    if (globalRegistry == null) {
      registerMetrics(registry);
      globalRegistry = registry;
    }
  }

  /**
   * 取消注册所有已注册的指标
   */
  public void unregister() {
    if (globalRegistry != null) {
      registeredMetricKeys.forEach(globalRegistry::unregister);
      registeredMetricKeys.clear();
      globalRegistry = null;
    }
  }

  /**
   * 抽象方法，由子类实现具体的指标注册逻辑
   * @param registry 指标注册器
   */
  protected abstract void registerMetrics(MetricRegistry registry);
}
