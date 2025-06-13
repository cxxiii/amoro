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

package org.apache.amoro.optimizer.standalone;

import org.apache.amoro.optimizer.common.Optimizer;
import org.apache.amoro.optimizer.common.OptimizerConfig;
import org.apache.amoro.resource.Resource;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/** 独立优化器主类，负责启动和管理优化器进程 */
public class StandaloneOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneOptimizer.class);

  /**
   * 主方法，启动独立优化器
   *
   * @param args 命令行参数，用于配置优化器
   * @throws CmdLineException 当命令行参数解析失败时抛出
   */
  public static void main(String[] args) throws CmdLineException {
    // 初始化优化器配置
    OptimizerConfig optimizerConfig = new OptimizerConfig(args);
    // 创建优化器实例
    Optimizer optimizer = new Optimizer(optimizerConfig);

    // // 计算优化器内存分配（单位：MB）
    LOG.info("Calculating optimizer available memory allocation...");
    long memorySize = Runtime.getRuntime().maxMemory() / 1024 / 1024;
    optimizerConfig.setMemorySize((int) memorySize);

    LOG.info("Obtaning current process id...");
    // 获取当前进程ID
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    String processId = runtimeMXBean.getName().split("@")[0];
    // 设置作业ID属性并启动优化器
    optimizer.getToucher().withRegisterProperty(Resource.PROPERTY_JOB_ID, processId);
    optimizer.startOptimizing();
  }
}
