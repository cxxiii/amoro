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

package org.apache.amoro.server.metrics;

import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegisterListener;
import org.apache.amoro.metrics.MetricSet;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** AMoro指标注册库，用于管理和跟踪所有注册的指标 实现了MetricSet接口，提供指标集合功能 */
public class MetricRegistry implements MetricSet {

  // 使用CopyOnWriteArrayList保证线程安全的监听器列表
  private final List<MetricRegisterListener> listeners = new CopyOnWriteArrayList<>();

  // 使用ConcurrentMap存储已注册的指标，保证线程安全
  private final ConcurrentMap<MetricKey, Metric> registeredMetrics = Maps.newConcurrentMap();
  // 存储指标定义及其引用计数
  private final Map<String, Pair<MetricDefine, Integer>> definedMetrics = Maps.newConcurrentMap();

  /**
   * 添加指标注册监听器
   *
   * @param listener 指标注册监听器
   */
  public void addListener(MetricRegisterListener listener) {
    this.listeners.add(listener);
  }

  /**
   * 注册一个新指标
   *
   * @param define 指标定义
   * @param tags 指标标签
   * @param metric 指标实例
   * @param <T> 指标类型
   * @return 注册成功的指标键
   * @throws IllegalArgumentException 如果指标已存在或定义不匹配
   */
  public <T extends Metric> MetricKey register(
      MetricDefine define, Map<String, String> tags, T metric) {
    // 参数校验
    Preconditions.checkNotNull(metric, "Metric must not be null");
    Preconditions.checkNotNull(define, "Metric define must not be null");
    Preconditions.checkArgument(
        define.getType().isType(metric),
        "Metric type miss-match, required：%s, but found implement:%s ",
        define.getType(),
        metric.getClass().getName());

    // 检查或创建指标定义
    Pair<MetricDefine, Integer> exists =
        definedMetrics.computeIfAbsent(define.getName(), name -> Pair.of(define, 0));
    Preconditions.checkArgument(
        exists.getLeft().equals(define),
        "The metric define with name: %s has been already exists, but the define is different.",
        define.getName());

    MetricKey key = new MetricKey(define, tags);

    // 原子性地更新指标定义和注册指标
    definedMetrics.computeIfPresent(
        define.getName(),
        (name, existsDefine) -> {
          Preconditions.checkArgument(
              define.equals(existsDefine.getLeft()),
              "Metric define:%s is not equal to existed define:%s",
              define,
              existsDefine.getLeft());
          Metric existedMetric = registeredMetrics.putIfAbsent(key, metric);
          Preconditions.checkArgument(existedMetric == null, "Metric is already been registered.");
          return Pair.of(existsDefine.getLeft(), existsDefine.getRight() + 1);
        });

    // 通知所有监听器
    callListener(listener -> listener.onMetricRegistered(key, metric));
    return key;
  }

  /**
   * 取消注册一个指标
   *
   * @param key 要取消注册的指标键
   */
  public void unregister(MetricKey key) {
    // 移除指标并通知监听器
    Metric exists = registeredMetrics.remove(key);
    if (exists != null) {
      callListener(listener -> listener.onMetricUnregistered(key));
    }
    // 更新指标定义的引用计数
    definedMetrics.computeIfPresent(
        key.getDefine().getName(),
        (name, pair) -> {
          int count = pair.getRight() - 1;
          if (count <= 0) {
            return null; // 引用计数为0时移除定义
          } else {
            return Pair.of(pair.getLeft(), count);
          }
        });
  }

  /**
   * 获取指定名称的指标定义计数（测试用）
   *
   * @param name 指标定义名称
   * @return 引用计数
   */
  @VisibleForTesting
  int metricDefineCount(String name) {
    return Optional.ofNullable(definedMetrics.getOrDefault(name, null))
        .map(Pair::getRight)
        .orElseGet(() -> 0);
  }

  /**
   * 获取所有已注册的指标（不可修改的视图）
   *
   * @return 指标映射
   */
  @Override
  public Map<MetricKey, Metric> getMetrics() {
    return Collections.unmodifiableMap(registeredMetrics);
  }

  /**
   * 通知所有监听器
   *
   * @param consumer 监听器回调函数
   */
  private void callListener(Consumer<MetricRegisterListener> consumer) {
    this.listeners.forEach(consumer);
  }
}
