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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.optimizing.MetricsSummary;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.dashboard.model.OverviewDataSizeItem;
import org.apache.amoro.server.dashboard.model.OverviewResourceUsageItem;
import org.apache.amoro.server.dashboard.model.OverviewSummary;
import org.apache.amoro.server.dashboard.model.OverviewTableOptimizingSummary;
import org.apache.amoro.server.dashboard.model.OverviewTopTableItem;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.CatalogMetaMapper;
import org.apache.amoro.server.persistence.mapper.OptimizerMapper;
import org.apache.amoro.server.persistence.mapper.OptimizingMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableList;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class OverviewManager extends PersistentBase {

    public static final String STATUS_PENDING = "Pending";
    public static final String STATUS_PLANING = "Planing";
    public static final String STATUS_EXECUTING = "Executing";
    public static final String STATUS_IDLE = "Idle";
    public static final String STATUS_COMMITTING = "Committing";

    private static final Logger LOG = LoggerFactory.getLogger(OverviewManager.class);
    private final List<OverviewTopTableItem> allTopTableItem = new ArrayList<>();
    private final Map<String, Long> optimizingStatusCountMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<OverviewResourceUsageItem> resourceUsageHistory =
            new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<OverviewDataSizeItem> dataSizeHistory =
            new ConcurrentLinkedDeque<>();
    private final Map<String, ConcurrentLinkedDeque<OverviewTableOptimizingSummary>>
            catalogOptimizingMap = new ConcurrentHashMap<>();
    private final AtomicInteger totalCpu = new AtomicInteger();
    private final AtomicLong totalMemory = new AtomicLong();
    private final Map<String, OverviewSummary> catalogSummaryMap = new ConcurrentHashMap<>();
    private final int maxRecordCount;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OverviewManager(Configurations serverConfigs) {
        this(
                serverConfigs.getInteger(AmoroManagementConf.OVERVIEW_CACHE_MAX_SIZE),
                serverConfigs.get(AmoroManagementConf.OVERVIEW_CACHE_REFRESH_INTERVAL));
    }

    @VisibleForTesting
    public OverviewManager(int maxRecordCount, Duration refreshInterval) {
        this.maxRecordCount = maxRecordCount;
        ScheduledExecutorService overviewUpdaterScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("overview-refresh-scheduler-%d")
                                .setDaemon(true)
                                .build());
        resetStatusMap();

        if (refreshInterval.toMillis() > 0) {
            overviewUpdaterScheduler.scheduleAtFixedRate(
                    this::refresh, 1000L, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public List<OverviewTopTableItem> getAllTopTableItem() {
        return ImmutableList.copyOf(allTopTableItem);
    }

    public OverviewSummary getCatalogOverviewSummary(String catalogName) {
        return catalogSummaryMap.get(catalogName);
    }

    public int getTotalCatalog() {
        return catalogSummaryMap.size();
    }

    public int getTotalTableCount() {
        return catalogSummaryMap.values().stream().mapToInt(OverviewSummary::getTableCnt).sum();
    }

    public long getTotalDataSize() {
        return catalogSummaryMap.values().stream().mapToLong(OverviewSummary::getTableTotalSize).sum();
    }

    public int getTotalCpu() {
        return totalCpu.get();
    }

    public long getTotalMemory() {
        return totalMemory.get();
    }

    public List<OverviewResourceUsageItem> getResourceUsageHistory(long startTime) {
        return resourceUsageHistory.stream()
                .filter(item -> item.getTs() >= startTime)
                .collect(Collectors.toList());
    }

    public List<OverviewDataSizeItem> getDataSizeHistory(long startTime) {
        return dataSizeHistory.stream()
                .filter(item -> item.getTs() >= startTime)
                .collect(Collectors.toList());
    }

    public OverviewTableOptimizingSummary getCatalogOptimizing(long startTime, String catalogName) {
        ConcurrentLinkedDeque<OverviewTableOptimizingSummary> catalogOptimizingSummary =
                catalogOptimizingMap.get(catalogName);

        if (catalogOptimizingSummary == null) {
            return new OverviewTableOptimizingSummary();
        }

        OverviewTableOptimizingSummary latest = catalogOptimizingSummary.peekLast();
        long latestRefreshTs = latest.getTs();
        List<OverviewTableOptimizingSummary> summaryList =
                catalogOptimizingSummary.stream()
                        .filter(item -> {
                            boolean condition1 = item.getTs() >= startTime + TimeUnit.HOURS.toMillis(1);
                            long diffMillis = latestRefreshTs - item.getTs();
                            boolean condition2 = diffMillis % TimeUnit.HOURS.toMillis(1) == 0;
                            return condition1 && condition2;
                        }).collect(Collectors.toList());

        AtomicLong totalMergeTaskCnt = new AtomicLong();
        AtomicLong totalInputDataSize = new AtomicLong();
        AtomicLong totalOutputDataSize = new AtomicLong();
        AtomicLong totalInputFileCnt = new AtomicLong();
        AtomicLong totalOutputFileCnt = new AtomicLong();

        for(OverviewTableOptimizingSummary summary : summaryList) {
            totalMergeTaskCnt.addAndGet(summary.getOptimizingProcessCount());
            totalInputFileCnt.addAndGet(summary.getOptimizingInputFileCount());
            totalInputDataSize.addAndGet(summary.getOptimizingInputDataSize());
            totalOutputFileCnt.addAndGet(summary.getOptimizingOutputFileCount());
            totalOutputDataSize.addAndGet(summary.getOptimizingOutputDataSize());
        }

        return new OverviewTableOptimizingSummary(totalMergeTaskCnt.get(), totalInputFileCnt.get(),
                totalInputDataSize.get(), totalOutputFileCnt.get(), totalOutputDataSize.get());
    }

    public Map<String, Long> getOptimizingStatus() {
        return optimizingStatusCountMap;
    }

    @VisibleForTesting
    public void refresh() {
        long start = System.currentTimeMillis();
        LOG.info("Refreshing overview cache");
        try {
            refreshTableCache(start);
            refreshResourceUsage(start);
            refreshCatalogCache(start);

        } catch (Exception e) {
            LOG.error("Refreshed overview cache failed", e);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("Refreshed overview cache in {} ms.", end - start);
        }
    }

    private void refreshTableCache(long ts) {
        List<CatalogMeta> catalogMetaList = getAs(CatalogMetaMapper.class, CatalogMetaMapper::getCatalogs);
        List<String> catalogList = catalogMetaList.stream().map(CatalogMeta::getCatalogName).collect(Collectors.toList());
        Map<String, Long> optimizingStatusMap = Maps.newHashMap();

        List<TableRuntimeMeta> allMetas =
                getAs(TableMetaMapper.class, TableMetaMapper::selectTableRuntimeMetas);
        for (String catalogName : catalogList) {
            AtomicLong totalDataSize = new AtomicLong();
            AtomicInteger totalFileCounts = new AtomicInteger();
            Map<String, OverviewTopTableItem> topTableItemMap = Maps.newHashMap();
            String optimizerGroup = null;

            for (TableRuntimeMeta meta : allMetas) {
                if(meta.getCatalogName().equals(catalogName)){
                    if(optimizerGroup == null){
                        optimizerGroup = meta.getOptimizerGroup();
                    }
                    Optional<OverviewTopTableItem> optItem = toTopTableItem(meta);
                    optItem.ifPresent(
                            tableItem -> {
                                topTableItemMap.put(tableItem.getTableName(), tableItem);
                                totalDataSize.addAndGet(tableItem.getTableSize());
                                totalFileCounts.addAndGet(tableItem.getFileCount());
                            });
                    String status = statusToMetricString(meta.getTableStatus());
                    if (StringUtils.isNotEmpty(status)) {
                        optimizingStatusMap.putIfAbsent(status, 0L);
                        optimizingStatusMap.computeIfPresent(status, (k, v) -> v + 1);
                    }
                }
            }
            catalogSummaryMap.putIfAbsent(catalogName, new OverviewSummary());
            catalogSummaryMap.get(catalogName).setCatalogCnt(1);
            catalogSummaryMap.get(catalogName).setTableCnt(topTableItemMap.size());
            catalogSummaryMap.get(catalogName).setTableTotalSize(totalDataSize.get());

            this.allTopTableItem.clear();
            this.allTopTableItem.addAll(topTableItemMap.values());

            List<OptimizerInstance> instances = getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
            AtomicInteger cpuCount = new AtomicInteger();
            AtomicLong memoryBytes = new AtomicLong();
            for (OptimizerInstance instance : instances) {
                if(instance.getGroupName().equals(optimizerGroup)){
                    cpuCount.addAndGet(instance.getThreadCount());
                    memoryBytes.addAndGet(instance.getMemoryMb() * 1024L * 1024L);
                }
            }
            catalogSummaryMap.get(catalogName).setTotalCpu(cpuCount.get());
            catalogSummaryMap.get(catalogName).setTotalMemory(memoryBytes.get());

        }

        addAndCheck(new OverviewDataSizeItem(ts, getTotalDataSize()));
        resetStatusMap();
        this.optimizingStatusCountMap.putAll(optimizingStatusMap);
    }

    private void refreshCatalogCache(long ts) {
        List<CatalogMeta> catalogMetaList = getAs(CatalogMetaMapper.class, CatalogMetaMapper::getCatalogs);
        List<String> catalogList = catalogMetaList.stream().map(CatalogMeta::getCatalogName).collect(Collectors.toList());
        AtomicLong totalMergeTaskCnt = new AtomicLong();
        AtomicLong totalInputDataSize = new AtomicLong();
        AtomicLong totalOutputDataSize = new AtomicLong();
        AtomicLong totalInputFileCnt = new AtomicLong();
        AtomicLong totalOutputFileCnt = new AtomicLong();

        for (String catalogName : catalogList) {
            totalInputFileCnt.set(0);
            totalOutputFileCnt.set(0);
            totalMergeTaskCnt.set(0);
            totalInputDataSize.set(0);
            totalOutputDataSize.set(0);
            catalogOptimizingMap.putIfAbsent(catalogName, new ConcurrentLinkedDeque<>());
            List<String> tableList =
                    getAs(TableMetaMapper.class, mapper -> mapper.getTableNames(catalogName));

            for (String tableName : tableList) {
                OverviewTableOptimizingSummary tableOptimizingSummary =
                        obtainCatalogOptimizingInfo(tableName, ts);
                totalMergeTaskCnt.addAndGet(tableOptimizingSummary.getOptimizingProcessCount());
                totalInputDataSize.addAndGet(tableOptimizingSummary.getOptimizingInputDataSize());
                totalOutputDataSize.addAndGet(tableOptimizingSummary.getOptimizingOutputDataSize());
                totalInputFileCnt.addAndGet(tableOptimizingSummary.getOptimizingInputFileCount());
                totalOutputFileCnt.addAndGet(tableOptimizingSummary.getOptimizingOutputFileCount());
            }
            OverviewTableOptimizingSummary catalogOptimizingSummary =
                    new OverviewTableOptimizingSummary(
                            ts,
                            totalMergeTaskCnt.get(),
                            totalInputFileCnt.get(),
                            totalInputDataSize.get(),
                            totalOutputFileCnt.get(),
                            totalOutputDataSize.get());
            addAndCheck(catalogOptimizingSummary, catalogName);
        }
    }

    private OverviewTableOptimizingSummary obtainCatalogOptimizingInfo(
            String fullTableName, long ts) {
        String catalog = fullTableName.split("\\.")[0];
        String db = fullTableName.split("\\.")[1];
        String table = fullTableName.split("\\.")[2];
        AtomicLong optimizingProcessCount = new AtomicLong();
        AtomicLong optimizingInputFileCount = new AtomicLong();
        AtomicLong optimizingInputDataSize = new AtomicLong();
        AtomicLong optimizingOutputFileCount = new AtomicLong();
        AtomicLong optimizingOutputDataSize = new AtomicLong();

        ProcessStatus status = ProcessStatus.SUCCESS;
        Timestamp endTime = new Timestamp(ts);
        Timestamp startTime = new Timestamp(ts - TimeUnit.HOURS.toMillis(1));
        List<String> jsonList = getAs(OptimizingMapper.class,
                        mapper -> mapper.selectProcessesMetrics(catalog, db, table, status, startTime, endTime));
        for (String json : jsonList) {
            MetricsSummary summary;
            try{
                 summary = objectMapper.readValue(json, MetricsSummary.class);
                if(summary!=null){
                    optimizingProcessCount.incrementAndGet();
                    optimizingInputFileCount.addAndGet(summary.getInputFilesStatistics().getFileCnt());
                    optimizingInputDataSize.addAndGet(summary.getInputFilesStatistics().getTotalSize());
                    optimizingOutputFileCount.addAndGet(summary.getOutputFilesStatistics().getFileCnt());
                    optimizingOutputDataSize.addAndGet(summary.getOutputFilesStatistics().getTotalSize());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return new OverviewTableOptimizingSummary(
                ts,
                optimizingProcessCount.get(),
                optimizingInputFileCount.get(),
                optimizingInputDataSize.get(),
                optimizingOutputFileCount.get(),
                optimizingOutputDataSize.get());
    }

    private Optional<OverviewTopTableItem> toTopTableItem(TableRuntimeMeta meta) {
        if (meta == null) {
            return Optional.empty();
        }
        OverviewTopTableItem tableItem = new OverviewTopTableItem(fullTableName(meta));
        if (meta.getTableSummary() != null) {
            tableItem.setTableSize(meta.getTableSummary().getTotalFileSize());
            tableItem.setFileCount(meta.getTableSummary().getTotalFileCount());
            tableItem.setHealthScore(meta.getTableSummary().getHealthScore());
        }
        tableItem.setAverageFileSize(
                tableItem.getFileCount() == 0 ? 0 : tableItem.getTableSize() / tableItem.getFileCount());
        return Optional.of(tableItem);
    }

    private String statusToMetricString(OptimizingStatus status) {
        if (status == null) {
            return null;
        }
        switch (status) {
            case PENDING:
                return STATUS_PENDING;
            case PLANNING:
                return STATUS_PLANING;
            case MINOR_OPTIMIZING:
            case MAJOR_OPTIMIZING:
            case FULL_OPTIMIZING:
                return STATUS_EXECUTING;
            case IDLE:
                return STATUS_IDLE;
            case COMMITTING:
                return STATUS_COMMITTING;
            default:
                return null;
        }
    }

    private void resetStatusMap() {
        optimizingStatusCountMap.clear();
        optimizingStatusCountMap.put(STATUS_PENDING, 0L);
        optimizingStatusCountMap.put(STATUS_PLANING, 0L);
        optimizingStatusCountMap.put(STATUS_EXECUTING, 0L);
        optimizingStatusCountMap.put(STATUS_IDLE, 0L);
        optimizingStatusCountMap.put(STATUS_COMMITTING, 0L);
    }

    private void refreshResourceUsage(long ts) {
        List<OptimizerInstance> instances = getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
        AtomicInteger cpuCount = new AtomicInteger();
        AtomicLong memoryBytes = new AtomicLong();
        for (OptimizerInstance instance : instances) {
            cpuCount.addAndGet(instance.getThreadCount());
            memoryBytes.addAndGet(instance.getMemoryMb() * 1024L * 1024L);
        }
        this.totalCpu.set(cpuCount.get());
        this.totalMemory.set(memoryBytes.get());
        addAndCheck(new OverviewResourceUsageItem(ts, cpuCount.get(), memoryBytes.get()));
    }

    private void addAndCheck(OverviewDataSizeItem dataSizeItem) {
        dataSizeHistory.add(dataSizeItem);
        checkSize(dataSizeHistory);
    }

    private void addAndCheck(OverviewResourceUsageItem resourceUsageItem) {
        resourceUsageHistory.add(resourceUsageItem);
        checkSize(resourceUsageHistory);
    }

    private void addAndCheck(
            OverviewTableOptimizingSummary catalogOptimizingSummary, String catalogName) {
        catalogOptimizingMap.get(catalogName).add(catalogOptimizingSummary);
        checkSize(catalogOptimizingMap.get(catalogName));
    }

    private <T> void checkSize(Deque<T> deque) {
        if (deque.size() > maxRecordCount) {
            deque.poll();
        }
    }

    private String fullTableName(TableRuntimeMeta meta) {
        return meta.getCatalogName()
                .concat(".")
                .concat(meta.getDbName())
                .concat(".")
                .concat(meta.getTableName());
    }
}
