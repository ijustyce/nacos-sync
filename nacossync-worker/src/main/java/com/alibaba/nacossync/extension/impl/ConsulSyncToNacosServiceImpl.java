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
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
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
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.ConsulUtils;
import com.alibaba.nacossync.util.NacosUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ObjectUtils;

/**
 * Consul 同步 Nacos
 *
 * @author paderlol
 * @date: 2018-12-31 16:25
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.CONSUL, destinationCluster = ClusterTypeEnum.NACOS)
public class ConsulSyncToNacosServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;
    private final ConsulServerHolder consulServerHolder;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final NacosServerHolder nacosServerHolder;
    private final ClusterAccessService clusterAccessService;
    private final SpecialSyncEventBus specialSyncEventBus;

    private final ConcurrentHashMap<String, Boolean> syncedService = new ConcurrentHashMap<>();

    @Autowired
    public ConsulSyncToNacosServiceImpl(ConsulServerHolder consulServerHolder,
                                        SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder,
                                        ClusterAccessService clusterAccessService, SpecialSyncEventBus specialSyncEventBus) {
        this.consulServerHolder = consulServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.clusterAccessService = clusterAccessService;
        this.specialSyncEventBus = specialSyncEventBus;
    }

    @Override
    public boolean delete(TaskDO taskDO) {

        try {
            log.info("delete nacos task id:{}", taskDO.getTaskId());
            specialSyncEventBus.unsubscribe(taskDO);

            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()));
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {

                    destNamingService.deregisterInstance(taskDO.getServiceName(),
                        NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()), instance.getIp(), instance.getPort());
                }
            }

        } catch (Exception e) {
            log.error("delete task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            doSync(taskDO);
            specialSyncEventBus.subscribe(taskDO, this::doSync);
        } catch (Exception e) {
            log.error("Sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    public void doSync(TaskDO taskDO) {
        long start = System.currentTimeMillis();
        try {
            ConsulClient consulClient = consulServerHolder.get(taskDO.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            Response<List<HealthService>> response =
                    consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServiceList = response.getValue();
            log.info("sourec ip list:{}", JSON.toJSONString(healthServiceList));
            Set<String> instanceKeys = new HashSet<>();
            overrideAllInstance(taskDO, destNamingService, healthServiceList, instanceKeys);
            cleanAllOldInstance(taskDO, destNamingService, instanceKeys);
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("Sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
        }
        log.info("doSync-finish takes {}", System.currentTimeMillis() - start);
    }

    private void cleanAllOldInstance(TaskDO taskDO, NamingService destNamingService, Set<String> instanceKeys)
        throws NacosException {
        String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
        List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName(),groupName);

        ClusterDO source = null;
        ClusterDO dest = null;

        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)
                && !instanceKeys.contains(composeInstanceKey(instance.getIp(), instance.getPort()))) {
                log.info("remove to nacos old ip:{}",instance.getIp());
                log.info("remove to nacos groupName:{}",groupName);
                destNamingService.deregisterInstance(taskDO.getServiceName(),
                        groupName, instance.getIp(), instance.getPort());
                log.info("remove to nacos taskInfo:{}",JSON.toJSONString(taskDO));

                if (source == null) {
                    source = clusterAccessService.findByClusterId(taskDO.getSourceClusterId());
                }

                if (dest == null) {
                    dest = clusterAccessService.findByClusterId(taskDO.getDestClusterId());
                }

                log.error("consul-nacos-diff 源集群名: {}, 目标集群名: {}, nacos 上存在来自同步的节点 serviceName {}, group {}, ip:port {}:{} " +
                                "但是 consul 上不存在该节点，现在删除它..", source == null ? "" : source.getClusterName(),
                        dest == null ? "" : dest.getClusterName(), taskDO.getServiceName(), groupName, instance.getIp(), instance.getPort());
            }
        }
    }

    private void overrideAllInstance(TaskDO taskDO, NamingService destNamingService,
        List<HealthService> healthServiceList, Set<String> instanceKeys) throws NacosException {
        String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
        List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName(),groupName);

        ClusterDO source = null;
        ClusterDO dest = null;

        for (HealthService healthService : healthServiceList) {
            if (needSync(ConsulUtils.transferMetadata(healthService.getService().getTags()))) {
                String address = healthService.getService().getAddress();
                int port = healthService.getService().getPort();
                instanceKeys.add(composeInstanceKey(address, port));

                Instance nacosInstance = findNacosInstance(allInstances, address, port);
                if (nacosInstance != null) {
                    Map<String, String> metadata = nacosInstance.getMetadata();
                    if (metadata == null || !metadata.containsKey(SkyWalkerConstants.SOURCE_CLUSTERID_KEY)) {
                        continue;
                    }
                }

                String instanceKey = taskDO.getTaskId() + "@@" + healthService.getService().getAddress()
                        + "_" + healthService.getService().getPort();
                if (syncedService.containsKey(instanceKey)) {
                    log.info("instanceKey {} exists return now", instanceKey);
                   // continue;
                }

                Instance instance = buildSyncInstance(healthService, taskDO);
                destNamingService.registerInstance(taskDO.getServiceName(), groupName, instance);
                syncedService.put(instanceKey, true);
                log.info("instanceKey {} not exists put to map", instanceKey);

                //  如果不在 nacos 里，则告警出来，同步是一定要去同步的
                if (nacosInstance == null) {
                    if (source == null) {
                        source = clusterAccessService.findByClusterId(taskDO.getSourceClusterId());
                    }

                    if (dest == null) {
                        dest = clusterAccessService.findByClusterId(taskDO.getDestClusterId());
                    }

                    log.error("consul-nacos-diff 源集群名: {}, 目标集群名: {}, nacos 上不存在 serviceName {}, group {}, ip:port {}:{} 现在开始同步.",
                            source == null ? "" : source.getClusterName(), dest == null ? "" : dest.getClusterName(),
                            taskDO.getServiceName(), groupName, instance.getIp(), instance.getPort());
                }
            }
        }
    }

    private Instance findNacosInstance(List<Instance> allInstances, String ip, int port) {
        if (ObjectUtils.isEmpty(allInstances)) {
            return null;
        }

        for (Instance instance : allInstances) {
            if (instance == null) {
                continue;
            }
            if (ip.equals(instance.getIp()) && port == instance.getPort()) {
                return instance;
            }
        }

        return null;
    }

    private Instance buildSyncInstance(HealthService instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getService().getAddress());
        temp.setPort(instance.getService().getPort());
        Map<String, String> metaData = new HashMap<>(ConsulUtils.transferMetadata(instance.getService().getTags()));
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        metaData.put("application", instance.getService().getService());
        metaData.put("timestamp", String.valueOf(System.currentTimeMillis()));
        temp.setMetadata(metaData);
        return temp;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

}
