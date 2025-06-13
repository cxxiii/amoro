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

package org.apache.amoro.server;

import org.apache.amoro.client.AmsServerInfo;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.properties.AmsHAProperties;
import org.apache.amoro.shade.zookeeper3.org.apache.curator.framework.CuratorFramework;
import org.apache.amoro.shade.zookeeper3.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.amoro.shade.zookeeper3.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.amoro.shade.zookeeper3.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.amoro.shade.zookeeper3.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.amoro.shade.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.amoro.shade.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.amoro.utils.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/** 高可用容器类，实现基于Zookeeper的Leader选举功能 实现了LeaderLatchListener接口，用于监听Leader状态变化 */
public class HighAvailabilityContainer implements LeaderLatchListener {

  public static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityContainer.class);

  private final LeaderLatch leaderLatch; // Leader选举器
  private final CuratorFramework zkClient; // Zookeeper客户端
  private final String tableServiceMasterPath; // 表服务Master节点路径
  private final String optimizingServiceMasterPath; // 优化服务Master节点路径
  private final AmsServerInfo tableServiceServerInfo; // 表服务服务器信息
  private final AmsServerInfo optimizingServiceServerInfo; // 优化服务服务器信息
  private volatile CountDownLatch followerLath; // 用于同步Follower状态的计数器

  /**
   * 构造函数，初始化高可用容器
   *
   * @param serviceConfig 服务配置
   * @throws Exception 初始化过程中可能抛出的异常
   */
  public HighAvailabilityContainer(Configurations serviceConfig) throws Exception {
    if (serviceConfig.getBoolean(AmoroManagementConf.HA_ENABLE)) {
      // 高可用模式下的初始化
      String zkServerAddress = serviceConfig.getString(AmoroManagementConf.HA_ZOOKEEPER_ADDRESS);
      String haClusterName = serviceConfig.getString(AmoroManagementConf.HA_CLUSTER_NAME);
      tableServiceMasterPath = AmsHAProperties.getTableServiceMasterPath(haClusterName);
      optimizingServiceMasterPath = AmsHAProperties.getOptimizingServiceMasterPath(haClusterName);

      // 配置Zookeeper重试策略
      ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
      this.zkClient =
          CuratorFrameworkFactory.builder()
              .connectString(zkServerAddress)
              .sessionTimeoutMs(5000)
              .connectionTimeoutMs(5000)
              .retryPolicy(retryPolicy)
              .build();
      zkClient.start();

      // 创建必要的Zookeeper路径
      createPathIfNeeded(tableServiceMasterPath);
      createPathIfNeeded(optimizingServiceMasterPath);
      String leaderPath = AmsHAProperties.getLeaderPath(haClusterName);
      createPathIfNeeded(leaderPath);

      // 初始化Leader选举器
      leaderLatch = new LeaderLatch(zkClient, leaderPath);
      leaderLatch.addListener(this);
      leaderLatch.start();

      // 构建服务器信息
      this.tableServiceServerInfo =
          buildServerInfo(
              serviceConfig.getString(AmoroManagementConf.SERVER_EXPOSE_HOST),
              serviceConfig.getInteger(AmoroManagementConf.TABLE_SERVICE_THRIFT_BIND_PORT),
              serviceConfig.getInteger(AmoroManagementConf.HTTP_SERVER_PORT));
      this.optimizingServiceServerInfo =
          buildServerInfo(
              serviceConfig.getString(AmoroManagementConf.SERVER_EXPOSE_HOST),
              serviceConfig.getInteger(AmoroManagementConf.OPTIMIZING_SERVICE_THRIFT_BIND_PORT),
              serviceConfig.getInteger(AmoroManagementConf.HTTP_SERVER_PORT));
    } else {
      // 非高可用模式下的初始化
      leaderLatch = null;
      zkClient = null;
      tableServiceMasterPath = null;
      optimizingServiceMasterPath = null;
      tableServiceServerInfo = null;
      optimizingServiceServerInfo = null;
      // 当高可用禁用时，永久阻塞follower latch
      followerLath = new CountDownLatch(1);
    }
  }

  /**
   * 等待当前服务实例成为Leader（主节点），并在成为Leader后更新Zookeeper上的服务信息
   *
   * @throws Exception 等待过程中可能抛出的异常
   */
  public void waitLeaderShip() throws Exception {
    LOG.info("Waiting to become the leader of AMS");
    if (leaderLatch != null) {
      leaderLatch.await();
      if (leaderLatch.hasLeadership()) {
        // 成为Leader后更新Zookeeper上的服务信息
        zkClient
            .setData()
            .forPath(
                tableServiceMasterPath,
                JacksonUtil.toJSONString(tableServiceServerInfo).getBytes(StandardCharsets.UTF_8));
        zkClient
            .setData()
            .forPath(
                optimizingServiceMasterPath,
                JacksonUtil.toJSONString(optimizingServiceServerInfo)
                    .getBytes(StandardCharsets.UTF_8));
      }
    }
    LOG.info("Became the leader of AMS");
  }

  /**
   * 等待成为Follower 让当前服务实例等待成为AMS(Arctic Meta Service)的Follower(从节点)
   * 主要用于高可用场景下，当服务实例从Leader降级为Follower时的状态同步
   *
   * @throws Exception 等待过程中可能抛出的异常
   */
  public void waitFollowerShip() throws Exception {
    LOG.info("Waiting to become the follower of AMS");
    if (followerLath != null) {
      followerLath.await();
    }
    LOG.info("Became the follower of AMS");
  }

  /** 关闭高可用服务 */
  public void close() {
    if (leaderLatch != null) {
      try {
        this.leaderLatch.close();
        this.zkClient.close();
      } catch (IOException e) {
        LOG.error("Close high availability services failed", e);
      }
    }
  }

  /** 成为Leader时的回调方法 */
  @Override
  public void isLeader() {
    LOG.info(
        "Table service server {} and optimizing service server {} got leadership",
        tableServiceServerInfo.toString(),
        optimizingServiceServerInfo.toString());
    followerLath = new CountDownLatch(1);
  }

  /** 失去Leader时的回调方法 */
  @Override
  public void notLeader() {
    LOG.info(
        "Table service server {} and optimizing service server {} lost leadership",
        tableServiceServerInfo.toString(),
        optimizingServiceServerInfo.toString());
    followerLath.countDown();
  }

  /**
   * 构建服务器信息对象
   *
   * @param host 主机地址
   * @param thriftBindPort Thrift绑定端口
   * @param restBindPort REST绑定端口
   * @return 构建好的服务器信息对象
   */
  private AmsServerInfo buildServerInfo(String host, int thriftBindPort, int restBindPort) {
    AmsServerInfo amsServerInfo = new AmsServerInfo();
    amsServerInfo.setHost(host);
    amsServerInfo.setRestBindPort(restBindPort);
    amsServerInfo.setThriftBindPort(thriftBindPort);
    return amsServerInfo;
  }

  /**
   * 在Zookeeper上创建路径（如果不存在）
   *
   * @param path 要创建的路径
   * @throws Exception 创建过程中可能抛出的异常
   */
  private void createPathIfNeeded(String path) throws Exception {
    try {
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
    } catch (KeeperException.NodeExistsException e) {
      // 忽略路径已存在的异常
    }
  }
}
