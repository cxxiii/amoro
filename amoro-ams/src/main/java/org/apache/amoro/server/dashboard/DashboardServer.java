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

package org.apache.amoro.server.dashboard;

import static io.javalin.apibuilder.ApiBuilder.delete;
import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.path;
import static io.javalin.apibuilder.ApiBuilder.post;
import static io.javalin.apibuilder.ApiBuilder.put;

import io.javalin.apibuilder.EndpointGroup;
import io.javalin.core.security.BasicAuthCredentials;
import io.javalin.http.ContentType;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.staticfiles.Location;
import io.javalin.http.staticfiles.StaticFileConfig;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.ForbiddenException;
import org.apache.amoro.exception.SignatureCheckException;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.DefaultOptimizingService;
import org.apache.amoro.server.RestCatalogService;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.CatalogController;
import org.apache.amoro.server.dashboard.controller.HealthCheckController;
import org.apache.amoro.server.dashboard.controller.LoginController;
import org.apache.amoro.server.dashboard.controller.OptimizerController;
import org.apache.amoro.server.dashboard.controller.OptimizerGroupController;
import org.apache.amoro.server.dashboard.controller.OverviewController;
import org.apache.amoro.server.dashboard.controller.PlatformFileInfoController;
import org.apache.amoro.server.dashboard.controller.SettingController;
import org.apache.amoro.server.dashboard.controller.TableController;
import org.apache.amoro.server.dashboard.controller.TerminalController;
import org.apache.amoro.server.dashboard.controller.VersionController;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.utils.ParamSignatureCalculator;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.terminal.TerminalManager;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class DashboardServer {

  public static final Logger LOG = LoggerFactory.getLogger(DashboardServer.class);

  private static final String AUTH_TYPE_BASIC = "basic";
  private static final String X_REQUEST_SOURCE_HEADER = "X-Request-Source";
  private static final String X_REQUEST_SOURCE_WEB = "Web";
  private final CatalogController catalogController;
  private final HealthCheckController healthCheckController;
  private final LoginController loginController;
  private final OptimizerGroupController optimizerGroupController;
  private final OptimizerController optimizerController;
  private final PlatformFileInfoController platformFileInfoController;
  private final SettingController settingController;
  private final TableController tableController;
  private final TerminalController terminalController;
  private final VersionController versionController;
  private final OverviewController overviewController;

  private final String authType;
  private final String basicAuthUser;
  private final String basicAuthPassword;

  public DashboardServer(
      Configurations serviceConfig,
      CatalogManager catalogManager,
      TableManager tableManager,
      OptimizerManager optimizerManager,
      DefaultOptimizingService optimizingService,
      TerminalManager terminalManager) {
    PlatformFileManager platformFileManager = new PlatformFileManager();
    this.catalogController = new CatalogController(catalogManager, platformFileManager);
    this.healthCheckController = new HealthCheckController();
    this.loginController = new LoginController(serviceConfig);
    // TODO: remove table service from OptimizerGroupController
    this.optimizerGroupController =
        new OptimizerGroupController(tableManager, optimizingService, optimizerManager);
    this.optimizerController = new OptimizerController(optimizingService, optimizerManager);
    this.platformFileInfoController = new PlatformFileInfoController(platformFileManager);
    this.settingController = new SettingController(serviceConfig, optimizerManager);
    ServerTableDescriptor tableDescriptor =
        new ServerTableDescriptor(catalogManager, tableManager, serviceConfig);
    // TODO: remove table service from TableController
    this.tableController =
        new TableController(catalogManager, tableManager, tableDescriptor, serviceConfig);
    this.terminalController = new TerminalController(terminalManager);
    this.versionController = new VersionController();
    OverviewManager manager = new OverviewManager(serviceConfig);
    this.overviewController = new OverviewController(manager);

    this.authType = serviceConfig.get(AmoroManagementConf.HTTP_SERVER_REST_AUTH_TYPE);
    this.basicAuthUser = serviceConfig.get(AmoroManagementConf.ADMIN_USERNAME);
    this.basicAuthPassword = serviceConfig.get(AmoroManagementConf.ADMIN_PASSWORD);
  }

  private volatile String indexHtml = null;
  // read index.html content
  public String getIndexFileContent() {
    if (indexHtml == null) {
      synchronized (this) {
        if (indexHtml == null) {
          try (InputStream inputStream =
              DashboardServer.class.getClassLoader().getResourceAsStream("static/index.html")) {
            Preconditions.checkNotNull(inputStream, "Cannot find index file.");
            try (InputStreamReader isr =
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(isr)) {
              StringBuilder sb = new StringBuilder();
              String line;
              while ((line = br.readLine()) != null) {
                sb.append(line);
              }
              indexHtml = sb.toString();
            }
          } catch (IOException e) {
            throw new UncheckedIOException("Load index html failed", e);
          }
        }
      }
    }
    return indexHtml;
  }

  public Consumer<StaticFileConfig> configStaticFiles() {
    return staticFiles -> {
      staticFiles.hostedPath = "/";
      // change to host files on a sub path, like '/assets'
      staticFiles.directory = "/static";
      // the directory where your files are located
      staticFiles.location = Location.CLASSPATH;
      // Location.CLASSPATH (jar) or Location.EXTERNAL (file system)
      staticFiles.precompress = false;
      // if the files should be pre-compressed and cached in memory (optimization)
      staticFiles.aliasCheck = null;
      // you can configure this to enable symlinks (= ContextHandler.ApproveAliases())
      // staticFiles.headers = Map.of(...);
      // headers that will be set for the files
      staticFiles.skipFileFunction = req -> false;
      // you can use this to skip certain files in the dir, based on the HttpServletRequest
    };
  }

  /**
   * 定义Dashboard服务的API端点路由配置
   *
   * @return EndpointGroup 包含所有路由配置的端点组
   */
  public EndpointGroup endpoints() {
    return () -> {
      /* 后端路由配置 */
      path(
          "",  // 根路径路由
          () -> {
            // Swagger文档路由
            get(
                "/swagger-docs",
                ctx -> {
                  // 从classpath加载OpenAPI规范文件
                  InputStream openapiStream =
                      getClass().getClassLoader().getResourceAsStream("openapi/openapi.yaml");
                  if (openapiStream == null) {
                    ctx.status(404).result("OpenAPI specification file not found");
                  } else {
                    ctx.result(openapiStream);
                  }
                });
            // 静态文件路由
            get(
                "/{page}",  // 动态路径参数
                ctx -> {
                  String fileName = ctx.pathParam("page");
                  if (fileName.endsWith("ico")) {  // 处理图标文件
                    ctx.contentType(ContentType.IMAGE_ICO);
                    ctx.result(
                        Objects.requireNonNull(
                            DashboardServer.class
                                .getClassLoader()
                                .getResourceAsStream("static/" + fileName)));
                  } else {  // 其他页面返回index.html内容
                    ctx.html(getIndexFileContent());
                  }
                });
            // Hive表升级页面路由
            get("/hive-tables/upgrade", ctx -> ctx.html(getIndexFileContent()));
          });

      // Dashboard API路由组
      path(
          "/api/ams/v1",  // API基础路径
          () -> {
            // 登录相关接口
            get("/login/current", loginController::getCurrent);  // 获取当前登录状态
            post("/login", loginController::login);              // 登录接口
            post("/logout", loginController::logout);            // 登出接口
          });

      // OpenAPI路由组
      path("/api/ams/v1", apiGroup());  // 复用API基础路径，使用apiGroup定义的路由
    };
  }

/**
 * 定义Dashboard服务的API端点组
 *
 * 该方法返回一个EndpointGroup，包含了Dashboard服务所有的API路由配置，
 * 按照功能模块分为表管理、目录管理、优化管理、终端操作、文件管理、设置管理等多个API组
 *
 * @return EndpointGroup 包含所有API路由配置的端点组
 */
private EndpointGroup apiGroup() {
    return () -> {
      // 表管理相关API
      path(
          "/tables",
          () -> {
            // 获取表详情
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/details", tableController::getTableDetail);
            // 获取Hive表详情
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/hive/details", tableController::getHiveTableDetail);
            // 升级Hive表
            post("/catalogs/{catalog}/dbs/{db}/tables/{table}/upgrade", tableController::upgradeHiveTable);
            // 获取升级状态
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/upgrade/status", tableController::getUpgradeStatus);
            // 获取优化进程列表
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/optimizing-processes", tableController::getOptimizingProcesses);
            // 获取优化类型
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/optimizing-types", tableController::getOptimizingTypes);
            // 获取优化进程任务
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/optimizing-processes/{processId}/tasks", tableController::getOptimizingProcessTasks);
            // 获取表快照
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/snapshots", tableController::getTableSnapshots);
            // 获取快照详情
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/snapshots/{snapshotId}/detail", tableController::getSnapshotDetail);
            // 获取表分区
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/partitions", tableController::getTablePartitions);
            // 获取分区文件列表
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/partitions/{partition}/files", tableController::getPartitionFileListInfo);
            // 获取表操作记录
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/operations", tableController::getTableOperations);
            // 获取表标签
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/tags", tableController::getTableTags);
            // 获取表分支
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/branches", tableController::getTableBranches);
            // 获取表消费者信息
            get("/catalogs/{catalog}/dbs/{db}/tables/{table}/consumers", tableController::getTableConsumerInfos);
            // 取消优化进程
            post("/catalogs/{catalog}/dbs/{db}/tables/{table}/optimizing-processes/{processId}/cancel", tableController::cancelOptimizingProcess);
          });
      // 获取升级属性
      get("/upgrade/properties", tableController::getUpgradeHiveTableProperties);

      // 目录管理相关API
      path(
          "/catalogs",
          () -> {
            // 获取表列表
            get("/{catalog}/databases/{db}/tables", tableController::getTableList);
            // 获取数据库列表
            get("/{catalog}/databases", tableController::getDatabaseList);
            // 获取目录列表
            get("", tableController::getCatalogs);
            // 创建目录
            post("", catalogController::createCatalog);
            // 获取目录类型列表
            get("metastore/types", catalogController::getCatalogTypeList);
            // 获取目录详情
            get("/{catalogName}", catalogController::getCatalogDetail);
            // 删除目录
            delete("/{catalogName}", catalogController::deleteCatalog);
            // 更新目录
            put("/{catalogName}", catalogController::updateCatalog);
            // 检查目录删除
            get("/{catalogName}/delete/check", catalogController::catalogDeleteCheck);
            // 获取目录配置文件内容
            get("/{catalogName}/config/{type}/{key}", catalogController::getCatalogConfFileContent);
          });

      // 优化管理相关API
      path(
          "/optimize",
          () -> {
            // 获取优化动作
            get("/actions", optimizerGroupController::getActions);
            // 获取优化组表
            get("/optimizerGroups/{optimizerGroup}/tables", optimizerGroupController::getOptimizerTables);
            // 获取优化器列表
            get("/optimizerGroups/{optimizerGroup}/optimizers", optimizerGroupController::getOptimizers);
            // 获取优化组列表
            get("/optimizerGroups", optimizerGroupController::getOptimizerGroups);
            // 获取优化组信息
            get("/optimizerGroups/{optimizerGroup}/info", optimizerGroupController::getOptimizerGroupInfo);
            // 扩展优化器
            post("/optimizerGroups/{optimizerGroup}/optimizers", optimizerGroupController::scaleOutOptimizer);
            // 创建优化器
            post("/optimizers", optimizerController::createOptimizer);
            // 释放优化器
            delete("/optimizers/{jobId}", optimizerController::releaseOptimizer);
            // 获取资源组
            get("/resourceGroups", optimizerGroupController::getResourceGroup);
            // 创建资源组
            post("/resourceGroups", optimizerGroupController::createResourceGroup);
            // 更新资源组
            put("/resourceGroups", optimizerGroupController::updateResourceGroup);
            // 删除资源组
            delete("/resourceGroups/{resourceGroupName}", optimizerGroupController::deleteResourceGroup);
            // 检查资源组删除
            get("/resourceGroups/{resourceGroupName}/delete/check", optimizerGroupController::deleteCheckResourceGroup);
            // 获取容器
            get("/containers/get", optimizerGroupController::getContainers);
          });

      // 终端操作相关API
      path(
          "/terminal",
          () -> {
            // 获取示例列表
            get("/examples", terminalController::getExamples);
            // 获取SQL示例
            get("/examples/{exampleName}", terminalController::getSqlExamples);
            // 执行脚本
            post("/catalogs/{catalog}/execute", terminalController::executeScript);
            // 获取日志
            get("/{sessionId}/logs", terminalController::getLogs);
            // 获取SQL结果
            get("/{sessionId}/result", terminalController::getSqlResult);
            // 停止SQL执行
            put("/{sessionId}/stop", terminalController::stopSql);
            // 获取最新信息
            get("/latestInfos/", terminalController::getLatestInfo);
          });

      // 文件管理相关API
      path(
          "/files",
          () -> {
            // 上传文件
            post("", platformFileInfoController::uploadFile);
            // 下载文件
            get("/{fileId}", platformFileInfoController::downloadFile);
          });

      // 设置管理相关API
      path(
          "/settings",
          () -> {
            // 获取容器设置
            get("/containers", settingController::getContainerSetting);
            // 获取系统设置
            get("/system", settingController::getSystemSetting);
          });

      // 健康检查API
      get("/health/status", healthCheckController::healthCheck);

      // 版本信息API
      get("/versionInfo", versionController::getVersionInfo);

      // 概览相关API
      path(
          "/overview",
          () -> {
            // 获取概要信息
            get("/summary", overviewController::getSummary);
            // 获取资源使用历史
            get("/resource", overviewController::getResourceUsageHistory);
            // 获取优化状态
            get("/optimizing", overviewController::getOptimizingStatus);
            // 获取数据大小历史
            get("/dataSize", overviewController::getDataSizeHistory);
            // 获取顶部表
            get("/top", overviewController::getTopTables);
          });
    };
  }

  public void preHandleRequest(Context ctx) {
    String uriPath = ctx.path();
    if (inWhiteList(uriPath)) {
      return;
    }
    String requestSource = ctx.header(X_REQUEST_SOURCE_HEADER);
    boolean isWebRequest = X_REQUEST_SOURCE_WEB.equalsIgnoreCase(requestSource);

    if (isWebRequest) {
      if (null == ctx.sessionAttribute("user")) {
        throw new ForbiddenException("User session attribute is missed for url: " + uriPath);
      }
      return;
    }
    if (AUTH_TYPE_BASIC.equalsIgnoreCase(authType)) {
      BasicAuthCredentials cred = ctx.basicAuthCredentials();
      if (!(basicAuthUser.equals(cred.component1())
          && basicAuthPassword.equals(cred.component2()))) {
        throw new SignatureCheckException(
            "Failed to authenticate via basic authentication for url:" + uriPath);
      }
    } else {
      checkApiToken(
          ctx.url(), ctx.queryParam("apiKey"), ctx.queryParam("signature"), ctx.queryParamMap());
    }
  }

  public void handleException(Exception e, Context ctx) {
    if (e instanceof ForbiddenException) {
      // request doesn't start with /ams is  page request. we return index.html
      if (!ctx.req.getRequestURI().startsWith("/api/ams")) {
        ctx.html(getIndexFileContent());
      } else {
        ctx.json(new ErrorResponse(HttpCode.FORBIDDEN, "Please login first", ""));
      }
    } else if (e instanceof SignatureCheckException) {
      ctx.json(new ErrorResponse(HttpCode.FORBIDDEN, "Signature check failed", ""));
    } else {
      ctx.json(new ErrorResponse(HttpCode.INTERNAL_SERVER_ERROR, e.getMessage(), ""));
    }
    LOG.error("An error occurred while processing the url:{}", ctx.url(), e);
  }

  private static final String[] URL_WHITE_LIST = {
    "/api/ams/v1/versionInfo",
    "/api/ams/v1/login",
    "/api/ams/v1/health/status",
    "/api/ams/v1/login/current",
    "/",
    "/overview",
    "/introduce",
    "/tables",
    "/optimizers",
    "/login",
    "/terminal",
    "/hive-tables/upgrade",
    "/hive-tables",
    "/index.html",
    "/favicon.ico",
    "/assets/*",
    "/openapi-ui",
    "/openapi-ui/*",
    "/swagger-docs",
    RestCatalogService.ICEBERG_REST_API_PREFIX + "/*"
  };

  private static boolean inWhiteList(String uri) {
    for (String item : URL_WHITE_LIST) {
      if (item.endsWith("*")) {
        if (uri.startsWith(item.substring(0, item.length() - 1))) {
          return true;
        }
      } else {
        if (uri.equals(item)) {
          return true;
        }
      }
    }
    return false;
  }

  private void checkApiToken(
      String requestUrl, String apiKey, String signature, Map<String, List<String>> params) {
    String plainText;
    String encryptString;
    String signCal;

    try {
      if (apiKey == null || signature == null) {
        throw new SignatureCheckException("API key or signature is missing");
      }
      APITokenManager apiTokenService = new APITokenManager();
      String secret = apiTokenService.getSecretByKey(apiKey);

      if (secret == null) {
        throw new SignatureCheckException("Invalid API key");
      }

      params.remove("apiKey");
      params.remove("signature");

      String paramString = ParamSignatureCalculator.generateParamStringWithValueList(params);

      if (StringUtils.isBlank(paramString)) {
        encryptString = ParamSignatureCalculator.SIMPLE_DATE_FORMAT.format(new Date());
      } else {
        encryptString = paramString;
      }

      plainText = String.format("%s%s%s", apiKey, encryptString, secret);
      signCal = ParamSignatureCalculator.getMD5(plainText);
      LOG.debug(
          "Calculated signature for url:{}, plain text:{}, calculated signature:{}, signature in request: {}",
          requestUrl,
          plainText,
          signCal,
          signature);

      if (!signature.equals(signCal)) {
        throw new SignatureCheckException(
            String.format(
                "Check signature for url:%s failed,"
                    + " calculated signature:%s, signature in request:%s",
                requestUrl, signCal, signature));
      }
    } catch (Exception e) {
      throw new SignatureCheckException("Check url signature failed", e);
    }
  }
}
