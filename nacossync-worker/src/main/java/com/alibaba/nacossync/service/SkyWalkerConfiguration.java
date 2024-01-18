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
package com.alibaba.nacossync.service;

import java.util.concurrent.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.EventBus;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerConfiguration.java, v 0.1 2018-09-27 AM1:20 NacosSync Exp $$
 */
@Configuration
@Slf4j
public class SkyWalkerConfiguration {

    @Bean
    public EventBus eventBus() {
        return new EventBus();
    }

    @Bean
    public ScheduledExecutorService executorService() {
        int corePollSize = Runtime.getRuntime().availableProcessors() + 1;
        corePollSize = corePollSize * 8;
        if (corePollSize < 32) {
            corePollSize = 32;
        }
        if (corePollSize > 64) {
            corePollSize = 64;
        }

        log.info("SkyWalker-Timer-schedule-pool core poll size {}", corePollSize);
        return new ScheduledThreadPoolExecutor(corePollSize, new BasicThreadFactory.Builder()
                .namingPattern("SkyWalker-Timer-schedule-pool-%d").daemon(true).build());
    }

    @Bean
    public ThreadPoolExecutor threadPoolExecutor() {
        int corePollSize = Runtime.getRuntime().availableProcessors();
        log.info("nacos-sync-pool availableProcessors size {}", corePollSize);
        corePollSize = corePollSize * 8;
        if (corePollSize < 16) {
            corePollSize = 16;
        }
        if (corePollSize > 64) {
            corePollSize = 64;
        }
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(2048);
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadFactory threadFactory = r -> {
            Thread thread = new Thread(r);
            thread.setName("nacos-sync-pool-" + thread.getId());
            return thread;
        };

        log.info("nacos-sync-pool core poll size {}", corePollSize);
        return new ThreadPoolExecutor(corePollSize, 320, 5, TimeUnit.MINUTES,
                blockingQueue, threadFactory, rejectedExecutionHandler);
    }

}
