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
package com.alibaba.nacossync.extension.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.MyConsulService;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.AlarmUtil;
import com.alibaba.nacossync.util.ConsulUtils;
import com.alibaba.nacossync.util.NacosUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.collect.Lists;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.CONSUL)
public class NacosSyncToConsulServiceImpl implements SyncService {
    public static final String ID_DELIMITER = "-";
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    private final MetricsManager metricsManager;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final NacosServerHolder nacosServerHolder;
    private final ConsulServerHolder consulServerHolder;
    private final SpecialSyncEventBus specialSyncEventBus;
    private final ClusterAccessService clusterAccessService;
    private final ThreadPoolExecutor threadPoolExecutor;

    public NacosSyncToConsulServiceImpl(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
                                        NacosServerHolder nacosServerHolder, ConsulServerHolder consulServerHolder,
                                        SpecialSyncEventBus specialSyncEventBus, ClusterAccessService clusterAccessService,
                                        ThreadPoolExecutor threadPoolExecutor) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.consulServerHolder = consulServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
        this.clusterAccessService = clusterAccessService;
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            log.info("delete consul task id:{}", taskDO.getTaskId());
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId());
            ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId());

            try {
                sourceNamingService.unsubscribe(taskDO.getServiceName(),
                        NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()), nacosListenerMap.get(taskDO.getTaskId()));
            } catch (Exception ignore) {
            }

            try {
                specialSyncEventBus.unsubscribe(taskDO);
            }catch (Exception ignore){}

            Response<List<HealthService>> serviceResponse =
                    consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServices = serviceResponse.getValue();
            log.info("delete-consul-result {}", healthServices);
            for (HealthService healthService : healthServices) {
                if (needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO)) {
                    log.info("do-delete-consul {}", healthService);
                    deleteService(consulClient, healthService);
                } else {
                    log.error("neeDelete is false taskDO {} tags {}", taskDO, healthService.getService().getTags());
                }
            }
        } catch (Exception e) {
            log.error("delete a task from nacos to consul was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        log.info("nacos-to-consul taskId {}", taskDO.getTaskId());
        try {
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId());

            EventListener listener = nacosListenerMap.computeIfAbsent(taskDO.getTaskId(), k -> event -> {
                if (event instanceof NamingEvent) {
                    log.info("onNamingEvent doSync taskId is {}", taskDO.getTaskId());
                    Future<?> future = threadPoolExecutor.submit(() -> doSync(taskDO));
                    try {
                        future.get(2, TimeUnit.MINUTES);
                    } catch (TimeoutException e) {
                        try {
                            future.cancel(true);
                        } catch (Exception ignore) {
                        }
                        log.error("sync-timeout", e);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("nacos-sync-to-consul failed after 2 minutes", e);
                    }
                }
            });

            String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
            log.info("unsubscribe for taskId {}", taskDO.getTaskId());

            try {
                sourceNamingService.unsubscribe(taskDO.getServiceName(), groupName, listener);
            } catch (Exception ignore) {
            }
            log.info("unsubscribe for taskId {}", taskDO.getTaskId());
            sourceNamingService.subscribe(taskDO.getServiceName(), groupName, listener);

            specialSyncEventBus.subscribe(taskDO, this::doSync);

        } catch (Exception e) {
            log.error("sync task from nacos to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    public void doSync(TaskDO taskDO) {
        long start = System.currentTimeMillis();
        try {
            log.info("do-sync taskId {}.", taskDO.getTaskId());
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId());
            ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId());

            Set<String> instanceKeySet = new HashSet<>();
            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                    NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()));
            log.info("sourec ip list:{} for taskId {}", JSON.toJSONString(sourceInstances), taskDO.getTaskId());
            Response<List<HealthService>> serviceResponse =
                    consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServices = serviceResponse.getValue();
            Set<String> ip2PortSet =
                    healthServices.stream().map(x -> composeInstanceKey(x.getService().getAddress(),
                            x.getService().getPort())).collect(Collectors.toSet());

            ClusterDO source = null;
            ClusterDO dest = null;

            // 先将新的注册一遍
            for (Instance instance : sourceInstances) {
                String ip2Port = composeInstanceKey(instance.getIp(), instance.getPort());
                log.info("source sync ip:{} for taskId {}", ip2Port, taskDO.getTaskId());
                if (needSync(instance.getMetadata())) {
                    log.info("need sync ip:{} for taskId {}", ip2Port, taskDO.getTaskId());
                    instanceKeySet.add(ip2Port);
                    if (!ip2PortSet.contains(ip2Port)) {

                        if (source == null) {
                            source = clusterAccessService.findByClusterId(taskDO.getSourceClusterId());
                        }

                        if (dest == null) {
                            dest = clusterAccessService.findByClusterId(taskDO.getDestClusterId());
                        }

                        log.error("nacos-consul-diff 源集群名: {}, 目标集群名: {}, consul 上不存在 serviceName {}, group {}, ip:port {} taskId {} 现在开始同步.",
                                source == null ? "" : source.getClusterName(), dest == null ? "" : dest.getClusterName(),
                                taskDO.getServiceName(), taskDO.getGroupName(), ip2Port, taskDO.getTaskId());
                        registerService(consulClient, buildSyncInstance(instance, taskDO));
                        log.info("already sync ip:{} for taskId {}", ip2Port, taskDO.getTaskId());
                    }

                }
            }

            // 再将不存在的删掉
            for (HealthService healthService : healthServices) {
                boolean needDelete = needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO);
                boolean notContain = !instanceKeySet.contains(composeInstanceKey(healthService.getService().getAddress(),
                        healthService.getService().getPort()));
                if (needDelete && notContain) {

                    if (source == null) {
                        source = clusterAccessService.findByClusterId(taskDO.getSourceClusterId());
                    }

                    if (dest == null) {
                        dest = clusterAccessService.findByClusterId(taskDO.getDestClusterId());
                    }

                    log.error("nacos-consul-diff 源集群名: {}, 目标集群名: {},  consul 上存在来自同步的节点 serviceName {}, group {}, ip:port {}:{} taskId {}" +
                                    "但是 nacos 上不存在该节点，现在删除它..",
                            source == null ? "" : source.getClusterName(), dest == null ? "" : dest.getClusterName(),
                            taskDO.getServiceName(), taskDO.getGroupName(), healthService.getService().getAddress(),
                            healthService.getService().getPort(), taskDO.getTaskId());
                    deleteService(consulClient, healthService);
                }
            }
        } catch (Exception e) {
            log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
        }
        log.info("doSync-finish takes {}", System.currentTimeMillis() - start);
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    public MyConsulService buildSyncInstance(Instance instance, TaskDO taskDO) {
        MyConsulService newService = new MyConsulService();
        newService.setAddress(instance.getIp());
        newService.setPort(instance.getPort());
        newService.setName(taskDO.getServiceName());
        if (StringUtils.isEmpty(instance.getInstanceId())) {
            instance.setInstanceId(generateInstanceId(instance));
        }
        newService.setId(instance.getInstanceId());
        List<String> tags = Lists.newArrayList();
        tags.addAll(instance.getMetadata().entrySet().stream()
                .map(entry -> String.join("=", entry.getKey(), entry.getValue())).collect(Collectors.toList()));
        tags.add(String.join("=", SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId()));
        tags.add(String.join("=", SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode()));
        tags.add(String.join("=", SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId()));
        newService.setTags(tags);
        newService.setNode("pod-" + instance.getIp().replaceAll("\\.", "-"));
        log.info("node is {}", newService.getNode());
        return newService;
    }

    private String generateInstanceId(Instance instance) {
        return instance.getIp() + ID_DELIMITER + instance.getPort() + ID_DELIMITER + instance.getServiceName();
    }

    private boolean deleteService(ConsulClient consulClient, HealthService healthService) {
        int count = 0;
        String serviceId = healthService.getService().getId();
        String encode = null;
        try {
            encode = URLEncoder.encode(serviceId, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                Response<Void> response = consulClient.agentServiceDeregister(encode);
                log.info("delete-consul-response {}", response);
                Response<List<HealthService>> serviceResponse =
                        consulClient.getHealthServices(healthService.getService().getService(), true, QueryParams.DEFAULT);
                List<HealthService> healthServices = serviceResponse.getValue();
                if (CollectionUtils.isEmpty(healthServices)) {
                    break;
                }
                Set<String> serviceIdSet =
                        healthServices.stream().map(x -> x.getService().getId()).collect(Collectors.toSet());
                if (CollectionUtils.isEmpty(serviceIdSet) || !serviceIdSet.contains(serviceId)) {
                    break;
                }
                count++;
                if (count > 10) {
                    log.error("Deregister failed");
                    AlarmUtil.alarm("Deregister failed,serviceId:" + serviceId);
                }
                if (count > 20) {
                    log.error("Deregister failed");
                    AlarmUtil.alarm("Deregister failed,serviceId:" + serviceId);
                    break;
                }
                Thread.sleep(50);
                consulClient.agentServiceDeregister(encode);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                count++;
                if (count > 10) {
                    log.error("Deregister failed");
                    AlarmUtil.alarm("Deregister failed,serviceId:" + serviceId);
                }
                if (count > 20) {
                    log.error("Deregister failed");
                    AlarmUtil.alarm("Deregister failed,serviceId:" + serviceId);
                    break;
                }
            }
        }

        return true;
    }

    private boolean registerService(ConsulClient consulClient, MyConsulService service) {
        int count = 0;
        String serviceId = service.getId();
        while (true) {
            try {
                consulClient.agentServiceRegister(service);
                count++;
                Response<List<HealthService>> serviceResponse =
                        consulClient.getHealthServices(service.getName(), true, QueryParams.DEFAULT);
                List<HealthService> healthServices = serviceResponse.getValue();
                if (CollectionUtils.isEmpty(healthServices)) {
                    if (count > 20) {
                        log.error("Register failed,serviceId:{}", serviceId);
                        AlarmUtil.alarm("Register failed,serviceId:" + serviceId);
                        break;
                    }
                    continue;
                }
                Set<String> serviceIdSet =
                        healthServices.stream().map(x -> x.getService().getId()).collect(Collectors.toSet());
                if (!CollectionUtils.isEmpty(serviceIdSet) && serviceIdSet.contains(serviceId)) {
                    break;
                }

                if (count > 10) {
                    log.error("Register failed,serviceId:{}", serviceId);
                    AlarmUtil.alarm("Register failed,serviceId:" + serviceId);
                }
                if (count > 20) {
                    log.error("Register failed,serviceId:{}", serviceId);
                    AlarmUtil.alarm("Register failed,serviceId:" + serviceId);
                    break;
                }
                Thread.sleep(50);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                count++;
                if (count > 10) {
                    log.error("Register failed,serviceId:{}", serviceId);
                    AlarmUtil.alarm("Register failed,serviceId:" + serviceId);
                }
                if (count > 20) {
                    log.error("Register failed,serviceId:{}", serviceId);
                    AlarmUtil.alarm("Register failed,serviceId:" + serviceId);
                    break;
                }
            }
        }

        return true;
    }
}
