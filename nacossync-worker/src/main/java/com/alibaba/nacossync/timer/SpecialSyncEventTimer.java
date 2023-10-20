/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.extension.event.SpecialSyncEvent;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * @author paderlol
 * @date: 2019-01-12 22:53
 */
@Slf4j
@Service
public class SpecialSyncEventTimer implements CommandLineRunner {

    @Autowired
    private SpecialSyncEventBus specialSyncEventBus;

    @Autowired
    private EventBus eventBus;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Override
    public void run(String... args) throws Exception {
        scheduledExecutorService.scheduleWithFixedDelay(new SpecialSyncEventTimer.SpecialSyncEventThread(), 0, 3000,
                TimeUnit.MILLISECONDS);
    }

    private class SpecialSyncEventThread implements Runnable {

        @Override
        public void run() {
            Future<?> future = threadPoolExecutor.submit(() -> {
                Collection<SpecialSyncEvent> allSpecialSyncEvent = specialSyncEventBus.getAllSpecialSyncEvent();
                allSpecialSyncEvent.stream()
                        .filter(specialSyncEvent -> TaskStatusEnum.SYNC.getCode()
                                .equals(specialSyncEvent.getTaskDO().getTaskStatus()))
                        .forEach(specialSyncEvent -> eventBus.post(specialSyncEvent));
            });
            try {
                future.get(90, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                try {
                    future.cancel(true);
                } catch (Exception ignore) {
                }
                log.error("sync-timeout", e);
            } catch (InterruptedException | ExecutionException e) {
                log.error("SpecialSyncEventThread error", e);
            }
        }
    }
}
