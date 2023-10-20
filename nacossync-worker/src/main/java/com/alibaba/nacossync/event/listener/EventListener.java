/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacossync.event.listener;

import javax.annotation.PostConstruct;

import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.monitor.MetricsManager;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import java.util.concurrent.*;

/**
 * @author NacosSync
 * @version $Id: EventListener.java, v 0.1 2018-09-27 AM1:21 NacosSync Exp $$
 */
@Slf4j
@Service
public class EventListener {

    @Autowired
    private MetricsManager metricsManager;

    @Autowired
    private SyncManagerService syncManagerService;

    @Autowired
    private EventBus eventBus;

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @PostConstruct
    public void register() {
        eventBus.register(this);
    }

    @Subscribe
    public void listenerSyncTaskEvent(SyncTaskEvent syncTaskEvent) {
        log.info("on SyncTaskEvent taskId {}", syncTaskEvent.getTaskDO().getTaskId());
//        Future<?> future = threadPoolExecutor.submit(() -> {
        threadPoolExecutor.execute(() -> {
            log.info("sync for taskId {}", syncTaskEvent.getTaskDO().getTaskId());
            try {
                long start = System.currentTimeMillis();
                if (syncManagerService.sync(syncTaskEvent.getTaskDO())) {
                    skyWalkerCacheServices.addFinishedTask(syncTaskEvent.getTaskDO());
                    long takes = System.currentTimeMillis() - start;
                    log.info("sync-finish {} takes {}", syncTaskEvent.getTaskDO().getTaskId(), takes);
                    metricsManager.record(MetricsStatisticsType.SYNC_TASK_RT, takes);
                } else {
                    log.warn("listenerSyncTaskEvent sync failure for taskId {}", syncTaskEvent.getTaskDO().getTaskId());
                }
            } catch (Exception e) {
                log.warn("listenerSyncTaskEvent process error for taskId {}", syncTaskEvent.getTaskDO().getTaskId(), e);
            }
        });

//        try {
//            future.get(30, TimeUnit.MINUTES);
//        } catch (TimeoutException e) {
//            log.error("listenerSyncTaskEvent-timeout", e);
//            try {
//                future.cancel(true);
//            }catch (Exception ignore){}
//        } catch (InterruptedException | ExecutionException e) {
//            log.error("listenerSyncTaskEvent-Exception", e);
//        }

    }

    @Subscribe
    public void listenerDeleteTaskEvent(DeleteTaskEvent deleteTaskEvent) {
//        Future<?> future = threadPoolExecutor.submit(() -> {
        threadPoolExecutor.execute(() -> {
            try {
                long start = System.currentTimeMillis();
                if (syncManagerService.delete(deleteTaskEvent.getTaskDO())) {
                    skyWalkerCacheServices.addFinishedTask(deleteTaskEvent.getTaskDO());
                    long takes = System.currentTimeMillis() - start;
                    log.info("delete-finish takes {}", takes);
                    metricsManager.record(MetricsStatisticsType.DELETE_TASK_RT, takes);
                } else {
                    log.warn("listenerDeleteTaskEvent delete failure");
                }
            } catch (Exception e) {
                log.warn("listenerDeleteTaskEvent process error", e);
            }
        });

//        try {
//            future.get(10, TimeUnit.MINUTES);
//        } catch (TimeoutException e) {
//            log.error("listenerDeleteTaskEvent-timeout", e);
//            try {
//                future.cancel(true);
//            } catch (Exception ignore) {
//            }
//        } catch (InterruptedException | ExecutionException e) {
//            log.error("listenerDeleteTaskEvent-Exception", e);
//        }
    }

}
