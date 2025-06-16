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

package org.apache.amoro.metrics;

import org.apache.amoro.shade.guava32.com.google.common.base.Joiner;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 指标键定义类，用于唯一标识一个注册的指标
 * 包含指标定义和对应的标签值集合
 */
public class MetricKey {

  private final MetricDefine define;       // 指标定义对象
  private final Map<String, String> valueOfTags;  // 标签名到标签值的映射

  /**
   * 构造函数
   * @param define 指标定义，不能为null
   * @param tagValues 标签值映射，可以为null（会被转换为空映射）
   * @throws IllegalArgumentException 如果标签值与指标定义不匹配
   */
  public MetricKey(MetricDefine define, Map<String, String> tagValues) {
    Preconditions.checkNotNull(define);
    this.valueOfTags = tagValues == null ? ImmutableMap.of() : tagValues;
    // 检查标签数量是否匹配
    if (define.getTags().size() != this.valueOfTags.size()) {
      throw new IllegalArgumentException(
          "Tag value is miss-match with metric define. MetricDefine{"
              + define
              + "} given tags:"
              + Joiner.on(",").join(this.valueOfTags.keySet().stream().sorted().iterator()));
    }

    // 检查每个标签是否都有对应的值
    define
        .getTags()
        .forEach(
            t ->
                Preconditions.checkArgument(
                    this.valueOfTags.containsKey(t), "The value of tag: %s is missed", t));
    this.define = define;
  }

  /**
   * 获取指标定义
   * @return 指标定义对象
   */
  public MetricDefine getDefine() {
    return define;
  }

  /**
   * 获取指定标签的值
   * @param tag 标签名
   * @return 标签值，如果不存在则返回空字符串
   */
  public String valueOfTag(String tag) {
    return valueOfTags.getOrDefault(tag, "");
  }

  /**
   * 获取所有标签的值列表
   * @return 不可修改的标签值列表（按定义中的标签顺序）
   */
  public List<String> valueOfTags() {
    List<String> valueOfTags = Lists.newArrayList();
    this.getDefine().getTags().forEach(t -> valueOfTags.add(valueOfTag(t)));
    return Collections.unmodifiableList(valueOfTags);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricKey that = (MetricKey) o;
    return Objects.equals(this.define, that.define)
        && Objects.equals(this.valueOfTags, that.valueOfTags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.define, this.valueOfTags);
  }

  @Override
  public String toString() {
    String desc = define.getName() + ":" + define.getType().name();
    if (!define.getTags().isEmpty()) {
      // 格式化为"<tag1=value1,tag2=value2>"的形式
      String tagDesc =
          define.getTags().stream()
              .map(t -> t + "=" + valueOfTag(t))
              .collect(Collectors.joining(","));
      desc = "<" + tagDesc + ">";
    }
    return desc;
  }
}
