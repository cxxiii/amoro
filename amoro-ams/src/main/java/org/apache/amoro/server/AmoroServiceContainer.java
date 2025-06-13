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

import io.javalin.Javalin;
import io.javalin.http.HttpCode;
import io.javalin.http.staticfiles.Location;
import org.apache.amoro.Constants;
import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.api.AmoroTableMetastore;
import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.config.ConfigHelpers;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.shade.utils.ConfigShadeUtils;
import org.apache.amoro.exception.AmoroRuntimeException;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.DefaultCatalogManager;
import org.apache.amoro.server.dashboard.DashboardServer;
import org.apache.amoro.server.dashboard.JavalinJsonMapper;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.utils.AmsUtil;
import org.apache.amoro.server.dashboard.utils.CommonUtil;
import org.apache.amoro.server.manager.EventsManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.DataSourceFactory;
import org.apache.amoro.server.persistence.HttpSessionHandlerFactory;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.resource.ContainerMetadata;
import org.apache.amoro.server.resource.DefaultOptimizerManager;
import org.apache.amoro.server.resource.InternalContainers;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.scheduler.inline.InlineTableExecutors;
import org.apache.amoro.server.table.DefaultTableManager;
import org.apache.amoro.server.table.DefaultTableService;
import org.apache.amoro.server.table.RuntimeHandlerChain;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableService;
import org.apache.amoro.server.terminal.TerminalManager;
import org.apache.amoro.server.utils.ThriftServiceProxy;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.shade.thrift.org.apache.thrift.TMultiplexedProcessor;
import org.apache.amoro.shade.thrift.org.apache.thrift.TProcessor;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocolFactory;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.THsHaServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportException;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportFactory;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.amoro.utils.IcebergThreadPools;
import org.apache.amoro.utils.JacksonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.sql.DataSource;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/** AMS服务容器类，负责管理AMS服务的生命周期和核心组件 */
public class AmoroServiceContainer {

  public static final Logger LOG = LoggerFactory.getLogger(AmoroServiceContainer.class);

  public static final String SERVER_CONFIG_FILENAME = "config.yaml";

  private final HighAvailabilityContainer haContainer; // 高可用容器
  private DataSource dataSource; // 数据源
  private CatalogManager catalogManager; // 目录管理器
  private TableManager tableManager; // 表管理器
  private OptimizerManager optimizerManager; // 优化器管理器
  private TableService tableService; // 表服务
  private DefaultOptimizingService optimizingService; // 优化服务
  private TerminalManager terminalManager; // 终端管理器
  private Configurations serviceConfig; // 服务配置
  private TServer tableManagementServer; // 表管理Thrift服务
  private TServer optimizingServiceServer; // 优化服务Thrift服务
  private Javalin httpServer; // HTTP服务
  private AmsServiceMetrics amsServiceMetrics; // 服务指标

  /**
   * 构造函数，初始化配置和高可用容器
   *
   * @throws Exception 初始化失败时抛出异常
   */
  public AmoroServiceContainer() throws Exception {
    initConfig();
    haContainer = new HighAvailabilityContainer(serviceConfig);
  }

  /**
   * 主入口方法
   *
   * @param args 命令行参数
   */
  public static void main(String[] args) {
    try {
      AmoroServiceContainer service = new AmoroServiceContainer();
      // 添加关闭钩子，用于在JVM关闭时优雅地停止服务
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    LOG.info("AMS service is shutting down...");
                    service.dispose();
                    LOG.info("AMS service has been shut down");
                  }));
      // 主循环
      while (true) {
        try {
          service.waitLeaderShip();
          service.startService();
          service.waitFollowerShip();
        } catch (Exception e) {
          LOG.error("AMS start error", e);
        } finally {
          service.dispose();
        }
      }
    } catch (Throwable t) {
      LOG.error("AMS encountered an unknown exception, will exist", t);
      System.exit(1);
    }
  }

  /**
   * 等待成为Leader节点
   *
   * @throws Exception 等待过程中出现异常
   */
  public void waitLeaderShip() throws Exception {
    haContainer.waitLeaderShip();
  }

  /**
   * 等待成为Follower节点
   *
   * @throws Exception 等待过程中出现异常
   */
  public void waitFollowerShip() throws Exception {
    haContainer.waitFollowerShip();
  }

  /**
   * 启动AMS服务
   *
   * @throws Exception 启动过程中出现异常
   */
  public void startService() throws Exception {
    // 初始化事件和指标管理器
    EventsManager.getInstance();
    MetricManager.getInstance();

    // 初始化核心管理器
    catalogManager = new DefaultCatalogManager(serviceConfig);
    tableManager = new DefaultTableManager(serviceConfig, catalogManager);
    optimizerManager = new DefaultOptimizerManager(serviceConfig, catalogManager);

    // 初始化表服务
    tableService = new DefaultTableService(serviceConfig, catalogManager);

    // 初始化优化服务
    optimizingService =
        new DefaultOptimizingService(serviceConfig, catalogManager, optimizerManager, tableService);

    // 设置表执行器并添加处理链
    LOG.info("Setting up AMS table executors...");
    InlineTableExecutors.getInstance().setup(tableService, serviceConfig);
    addHandlerChain(optimizingService.getTableRuntimeHandler());
    addHandlerChain(InlineTableExecutors.getInstance().getDataExpiringExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getSnapshotsExpiringExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getOrphanFilesCleaningExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getDanglingDeleteFilesCleaningExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getOptimizingCommitExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getOptimizingExpiringExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getBlockerExpiringExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getHiveCommitSyncExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getTableRefreshingExecutor());
    addHandlerChain(InlineTableExecutors.getInstance().getTagsAutoCreatingExecutor());

    // 初始化表服务
    tableService.initialize();
    LOG.info("AMS table service have been initialized");
    tableManager.setTableService(tableService);

    // 初始化终端管理器
    terminalManager = new TerminalManager(serviceConfig, catalogManager);

    // 初始化并启动Thrift服务
    initThriftService();
    startThriftService();

    // 初始化并启动HTTP服务
    initHttpService();
    startHttpService();

    // 注册服务指标
    registerAmsServiceMetric();
  }

  /**
   * 添加运行时处理链
   *
   * @param chain 处理链对象
   */
  private void addHandlerChain(RuntimeHandlerChain chain) {
    if (chain != null) {
      tableService.addHandlerChain(chain);
    }
  }

  /** 释放资源并停止服务 */
  public void dispose() {
    // 停止表管理Thrift服务
    if (tableManagementServer != null && tableManagementServer.isServing()) {
      LOG.info("Stopping table management server...");
      tableManagementServer.stop();
    }

    // 停止优化服务Thrift服务
    if (optimizingServiceServer != null && optimizingServiceServer.isServing()) {
      LOG.info("Stopping optimizing server...");
      optimizingServiceServer.stop();
    }

    // 停止HTTP服务
    if (httpServer != null) {
      LOG.info("Stopping http server...");
      try {
        httpServer.close();
      } catch (Exception e) {
        LOG.error("Error stopping http server", e);
      }
    }

    // 释放表服务资源
    if (tableService != null) {
      LOG.info("Stopping table service...");
      tableService.dispose();
      tableService = null;
    }

    // 释放终端管理器资源
    if (terminalManager != null) {
      LOG.info("Stopping terminal manager...");
      terminalManager.dispose();
      terminalManager = null;
    }

    // 释放优化服务资源
    if (optimizingService != null) {
      LOG.info("Stopping optimizing service...");
      optimizingService.dispose();
      optimizingService = null;
    }

    // 取消注册服务指标
    if (amsServiceMetrics != null) {
      amsServiceMetrics.unregister();
    }

    // 释放事件和指标管理器
    EventsManager.dispose();
    MetricManager.dispose();
  }

  /**
   * 初始化配置
   *
   * @throws Exception 初始化失败时抛出异常
   */
  private void initConfig() throws Exception {
    LOG.info("initializing configurations...");
    new ConfigurationHelper().init();
  }

  /** 启动Thrift服务 */
  private void startThriftService() {
    startThriftServer(tableManagementServer, "thrift-table-management-server-thread");
    startThriftServer(optimizingServiceServer, "thrift-optimizing-server-thread");
  }

  /**
   * 启动单个Thrift服务
   *
   * @param server Thrift服务实例
   * @param threadName 线程名称
   */
  private void startThriftServer(TServer server, String threadName) {
    Thread thread = new Thread(server::serve, threadName);
    thread.setDaemon(true);
    thread.start();
    LOG.info("{} has been started", threadName);
  }

  /** 初始化HTTP服务 */
  private void initHttpService() {
    // 创建Dashboard服务
    DashboardServer dashboardServer =
        new DashboardServer(
            serviceConfig,
            catalogManager,
            tableManager,
            optimizerManager,
            optimizingService,
            terminalManager);

    // 创建REST目录服务
    RestCatalogService restCatalogService = new RestCatalogService(catalogManager, tableManager);

    // 配置Javalin HTTP服务器
    httpServer =
        Javalin.create(
            config -> {
              // 配置静态文件
              config.addStaticFiles(dashboardServer.configStaticFiles());
              config.addStaticFiles("/META-INF/resources/webjars", Location.CLASSPATH);
              // 配置会话处理器
              config.sessionHandler(
                  () -> HttpSessionHandlerFactory.createSessionHandler(dataSource, serviceConfig));
              // 启用CORS
              config.enableCorsForAllOrigins();
              // 配置JSON映射器
              config.jsonMapper(JavalinJsonMapper.createDefaultJsonMapper());
              config.showJavalinBanner = false;
              config.enableWebjars();
            });

    // 添加路由
    httpServer.routes(
        () -> {
          dashboardServer.endpoints().addEndpoints();
          restCatalogService.endpoints().addEndpoints();
        });

    // 请求前置处理
    httpServer.before(
        ctx -> {
          String token = ctx.queryParam("token");
          if (StringUtils.isNotEmpty(token)) {
            CommonUtil.checkSinglePageToken(ctx);
          } else {
            dashboardServer.preHandleRequest(ctx);
          }
        });

    // 异常处理
    httpServer.exception(
        Exception.class,
        (e, ctx) -> {
          if (restCatalogService.needHandleException(ctx)) {
            restCatalogService.handleException(e, ctx);
          } else {
            dashboardServer.handleException(e, ctx);
          }
        });

    // 404错误处理
    httpServer.error(
        HttpCode.NOT_FOUND.getStatus(),
        ctx -> {
          if (!restCatalogService.needHandleException(ctx)) {
            ctx.json(new ErrorResponse(HttpCode.NOT_FOUND, "page not found!", ""));
          }
        });

    // 500错误处理
    httpServer.error(
        HttpCode.INTERNAL_SERVER_ERROR.getStatus(),
        ctx -> {
          if (!restCatalogService.needHandleException(ctx)) {
            ctx.json(new ErrorResponse(HttpCode.INTERNAL_SERVER_ERROR, "internal error!", ""));
          }
        });
  }

  /** 启动HTTP服务 */
  private void startHttpService() {
    int port = serviceConfig.getInteger(AmoroManagementConf.HTTP_SERVER_PORT);
    httpServer.start(port);

    // 打印启动logo
    LOG.info(
        "\n"
            + "    ___     __  ___ ____   ____   ____ \n"
            + "   /   |   /  |/  // __ \\ / __ \\ / __ \\\n"
            + "  / /| |  / /|_/ // / / // /_/ // / / /\n"
            + " / ___ | / /  / // /_/ // _, _// /_/ / \n"
            + "/_/  |_|/_/  /_/ \\____//_/ |_| \\____/  \n"
            + "                                       \n"
            + "      https://amoro.apache.org/       \n");

    LOG.info("Http server start at {}.", port);
  }

  /** 注册AMS服务指标 */
  private void registerAmsServiceMetric() {
    amsServiceMetrics = new AmsServiceMetrics(MetricManager.getInstance().getGlobalRegistry());
    amsServiceMetrics.register();
  }

  /**
   * 初始化Thrift服务
   *
   * @throws TTransportException Thrift传输异常
   */
  private void initThriftService() throws TTransportException {
    LOG.info("Initializing thrift service...");
    // 获取配置参数
    long maxMessageSize = serviceConfig.get(AmoroManagementConf.THRIFT_MAX_MESSAGE_SIZE).getBytes();
    int selectorThreads = serviceConfig.getInteger(AmoroManagementConf.THRIFT_SELECTOR_THREADS);
    int workerThreads = serviceConfig.getInteger(AmoroManagementConf.THRIFT_WORKER_THREADS);
    int queueSizePerSelector =
        serviceConfig.getInteger(AmoroManagementConf.THRIFT_QUEUE_SIZE_PER_THREAD);
    String bindHost = serviceConfig.getString(AmoroManagementConf.SERVER_BIND_HOST);

    // 创建表管理Thrift处理器
    AmoroTableMetastore.Processor<AmoroTableMetastore.Iface> tableManagementProcessor =
        new AmoroTableMetastore.Processor<>(
            ThriftServiceProxy.createProxy(
                AmoroTableMetastore.Iface.class,
                new TableManagementService(catalogManager, tableManager),
                AmoroRuntimeException::normalizeCompatibly));
    // 创建表管理Thrift服务
    tableManagementServer =
        createThriftServer(
            tableManagementProcessor,
            Constants.THRIFT_TABLE_SERVICE_NAME,
            bindHost,
            serviceConfig.getInteger(AmoroManagementConf.TABLE_SERVICE_THRIFT_BIND_PORT),
            Executors.newFixedThreadPool(
                workerThreads, getThriftThreadFactory(Constants.THRIFT_TABLE_SERVICE_NAME)),
            selectorThreads,
            queueSizePerSelector,
            maxMessageSize);

    // 创建优化服务Thrift处理器
    OptimizingService.Processor<OptimizingService.Iface> optimizingProcessor =
        new OptimizingService.Processor<>(
            ThriftServiceProxy.createProxy(
                OptimizingService.Iface.class,
                optimizingService,
                AmoroRuntimeException::normalize));
    // 创建优化服务Thrift服务
    optimizingServiceServer =
        createThriftServer(
            optimizingProcessor,
            Constants.THRIFT_OPTIMIZING_SERVICE_NAME,
            bindHost,
            serviceConfig.getInteger(AmoroManagementConf.OPTIMIZING_SERVICE_THRIFT_BIND_PORT),
            Executors.newCachedThreadPool(
                getThriftThreadFactory(Constants.THRIFT_OPTIMIZING_SERVICE_NAME)),
            selectorThreads,
            queueSizePerSelector,
            maxMessageSize);
  }

  /**
   * 创建Thrift服务实例
   *
   * @param processor Thrift处理器
   * @param processorName 处理器名称
   * @param bindHost 绑定主机
   * @param port 端口号
   * @param executorService 线程池
   * @param selectorThreads 选择器线程数
   * @param queueSizePerSelector 每个选择器的队列大小
   * @param maxMessageSize 最大消息大小
   * @return Thrift服务实例
   * @throws TTransportException Thrift传输异常
   */
  private TServer createThriftServer(
      TProcessor processor,
      String processorName,
      String bindHost,
      int port,
      ExecutorService executorService,
      int selectorThreads,
      int queueSizePerSelector,
      long maxMessageSize)
      throws TTransportException {
    LOG.info("Initializing thrift server: {}", processorName);
    LOG.info("Starting {} thrift server on port: {}", processorName, port);
    // 创建服务器传输
    TNonblockingServerSocket serverTransport = getServerSocket(bindHost, port);
    // 创建协议工厂
    final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    final TProtocolFactory inputProtoFactory =
        new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);
    // 创建传输工厂
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    // 创建多路复用处理器
    TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
    multiplexedProcessor.registerProcessor(processorName, processor);
    // 配置服务器参数
    THsHaServer.Args args =
        new THsHaServer.Args(serverTransport)
            .processor(multiplexedProcessor)
            .transportFactory(transportFactory)
            .protocolFactory(protocolFactory)
            .inputProtocolFactory(inputProtoFactory)
            .executorService(executorService);
    LOG.info(
        "The number of selector threads for the {} thrift server is: {}",
        processorName,
        selectorThreads);
    LOG.info(
        "The size of per-selector queue for the {} thrift server is: {}",
        processorName,
        queueSizePerSelector);
    return new THsHaServer(args);
  }

  /**
   * 获取Thrift线程工厂
   *
   * @param processorName 处理器名称
   * @return 线程工厂实例
   */
  private ThreadFactory getThriftThreadFactory(String processorName) {
    return new ThreadFactoryBuilder()
        .setDaemon(false)
        .setNameFormat(
            "thrift-server-"
                + String.join("-", StringUtils.splitByCharacterTypeCamelCase(processorName))
                    .toLowerCase(Locale.ROOT)
                + "-%d")
        .build();
  }

  /** 配置帮助类，用于加载和初始化配置 */
  private class ConfigurationHelper {

    private JsonNode yamlConfig; // YAML配置节点

    /**
     * 初始化配置
     *
     * @throws Exception 初始化失败时抛出异常
     */
    public void init() throws Exception {
      Map<String, Object> envConfig = initEnvConfig();
      initServiceConfig(envConfig);
      setIcebergSystemProperties();
      initContainerConfig();
    }

    /**
     * 初始化服务配置
     *
     * @param envConfig 环境变量配置
     * @throws Exception 初始化失败时抛出异常
     */
    private void initServiceConfig(Map<String, Object> envConfig) throws Exception {
      LOG.info("initializing service configuration...");
      String configPath = Environments.getConfigPath() + "/" + SERVER_CONFIG_FILENAME;
      LOG.info("load config from path: {}", configPath);
      // 加载YAML配置
      yamlConfig =
          JacksonUtil.fromObjects(
              new Yaml().loadAs(Files.newInputStream(Paths.get(configPath)), Map.class));
      // 获取系统配置
      Map<String, Object> systemConfig =
          JacksonUtil.getMap(
              yamlConfig,
              AmoroManagementConf.SYSTEM_CONFIG,
              new TypeReference<Map<String, Object>>() {});
      // 展开配置映射
      Map<String, Object> expandedConfigurationMap = Maps.newHashMap();
      expandConfigMap(systemConfig, "", expandedConfigurationMap);
      // 合并环境变量配置(环境变量优先级更高)
      expandedConfigurationMap.putAll(envConfig);
      // 解密敏感配置
      expandedConfigurationMap = ConfigShadeUtils.decryptConfig(expandedConfigurationMap);
      // 创建服务配置
      serviceConfig = Configurations.fromObjectMap(expandedConfigurationMap);
      // 验证配置
      AmoroManagementConfValidator.validateConfig(serviceConfig);
      // 初始化数据源
      dataSource = DataSourceFactory.createDataSource(serviceConfig);
      // 初始化SQL会话工厂
      SqlSessionFactoryProvider.getInstance().init(dataSource);
    }

    /**
     * 初始化环境变量配置
     *
     * @return 环境变量配置映射
     */
    private Map<String, Object> initEnvConfig() {
      LOG.info("initializing system env configuration...");
      Map<String, String> envs = System.getenv();
      envs.forEach((k, v) -> LOG.info("export {}={}", k, v));
      String prefix = AmoroManagementConf.SYSTEM_CONFIG.toUpperCase();
      return ConfigHelpers.convertConfigurationKeys(prefix, System.getenv());
    }

    /** 设置Iceberg系统属性 */
    private void setIcebergSystemProperties() {
      // 计算并设置工作线程池大小
      int workerThreadPoolSize =
          Math.max(
              Runtime.getRuntime().availableProcessors() / 2,
              serviceConfig.getInteger(AmoroManagementConf.TABLE_MANIFEST_IO_THREAD_COUNT));
      System.setProperty(
          SystemProperties.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(workerThreadPoolSize));

      // 计算并设置规划线程池大小
      int planningThreadPoolSize =
          Math.max(
              Runtime.getRuntime().availableProcessors() / 2,
              serviceConfig.getInteger(
                  AmoroManagementConf.TABLE_MANIFEST_IO_PLANNING_THREAD_COUNT));
      // 计算并设置提交线程池大小
      int commitThreadPoolSize =
          Math.max(
              Runtime.getRuntime().availableProcessors() / 2,
              serviceConfig.getInteger(AmoroManagementConf.TABLE_MANIFEST_IO_COMMIT_THREAD_COUNT));
      // 初始化Iceberg线程池
      IcebergThreadPools.init(planningThreadPoolSize, commitThreadPoolSize);
    }

    /** 初始化容器配置 */
    private void initContainerConfig() {
      LOG.info("initializing container configuration...");
      JsonNode containers = yamlConfig.get(AmoroManagementConf.CONTAINER_LIST);
      List<ContainerMetadata> containerList = new ArrayList<>();
      if (containers != null && containers.isArray()) {
        for (final JsonNode containerConfig : containers) {
          // 创建容器元数据
          ContainerMetadata container =
              new ContainerMetadata(
                  containerConfig.get(AmoroManagementConf.CONTAINER_NAME).asText(),
                  containerConfig.get(AmoroManagementConf.CONTAINER_IMPL).asText());

          // 获取容器属性
          Map<String, String> containerProperties =
              new HashMap<>(
                  JacksonUtil.getMap(
                      containerConfig,
                      AmoroManagementConf.CONTAINER_PROPERTIES,
                      new TypeReference<Map<String, String>>() {}));

          // 设置基础属性
          containerProperties.put(OptimizerProperties.AMS_HOME, Environments.getHomePath());
          containerProperties.putIfAbsent(
              OptimizerProperties.AMS_OPTIMIZER_URI,
              AmsUtil.getAMSThriftAddress(serviceConfig, Constants.THRIFT_OPTIMIZING_SERVICE_NAME));

          // 设置容器属性
          container.setProperties(containerProperties);
          containerList.add(container);
        }
      }
      // 初始化内部容器
      InternalContainers.init(containerList);
    }
  }

  /**
   * 获取服务器套接字
   *
   * @param bindHost 绑定主机
   * @param portNum 端口号
   * @return 非阻塞服务器套接字
   * @throws TTransportException Thrift传输异常
   */
  private TNonblockingServerSocket getServerSocket(String bindHost, int portNum)
      throws TTransportException {
    InetSocketAddress serverAddress;
    serverAddress = new InetSocketAddress(bindHost, portNum);
    return new TNonblockingServerSocket(serverAddress);
  }

  /**
   * 展开配置映射
   *
   * @param config 原始配置
   * @param prefix 前缀
   * @param result 结果映射
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public static void expandConfigMap(
      Map<String, Object> config, String prefix, Map<String, Object> result) {
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
      if (value instanceof Map) {
        Map<String, Object> subMap = (Map<String, Object>) value;
        expandConfigMap(subMap, fullKey, result);
      } else {
        result.put(fullKey, value);
      }
    }
  }

  /**
   * 获取表服务实例(测试用)
   *
   * @return 表服务实例
   */
  @VisibleForTesting
  public TableService getTableService() {
    return this.tableService;
  }

  /**
   * 获取目录管理器实例(测试用)
   *
   * @return 目录管理器实例
   */
  @VisibleForTesting
  public CatalogManager getCatalogManager() {
    return this.catalogManager;
  }

  /**
   * 获取优化器管理器实例(测试用)
   *
   * @return 优化器管理器实例
   */
  @VisibleForTesting
  public OptimizerManager getOptimizerManager() {
    return this.optimizerManager;
  }
}
