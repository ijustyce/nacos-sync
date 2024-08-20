package com.alibaba.nacossync.service;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author 杨春 At 2022-06-21 15:50
 */

@Slf4j
@Service
public class ToolsService {

    private final HttpService httpService;
    private final TaskAccessService taskAccessService;
    private final ClusterAccessService clusterAccessService;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final ScheduledExecutorService scheduledService;

    public ToolsService(HttpService httpService, TaskAccessService taskAccessService,
                        ClusterAccessService clusterAccessService,
                        SkyWalkerCacheServices skyWalkerCacheServices) {
        this.httpService = httpService;
        this.taskAccessService = taskAccessService;
        this.clusterAccessService = clusterAccessService;
        this.skyWalkerCacheServices = skyWalkerCacheServices;

        this.scheduledService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("asyncNacosService-" + thread.getId());
            return thread;
        });
    }

    public void tryToStartAsync(String sourceClusterId, String destClusterId) {
        log.info("begin runNacosServiceAsync");
        scheduledService.execute(() -> {
            try {
                asyncNacosServices(sourceClusterId, destClusterId);
            } catch (Exception e) {
                log.error("error while async nacos service", e);
            }
        });

        scheduledService.schedule(() -> tryToStartAsync(sourceClusterId, destClusterId), 5, TimeUnit.SECONDS);
        log.info("end runNacosServiceAsync");
    }

    private void asyncNacosServices(String sourceClusterId, String destClusterId) throws Exception {
        log.info("begin asyncNacosServices");

        ClusterDO sourceCluster = clusterAccessService.findByClusterId(sourceClusterId);

        String sourceServices = serviceInfo(sourceCluster);

        ArrayList<TaskDO> sourceTasks = servicesToTask(sourceServices, sourceClusterId, destClusterId);

        log.info("sources task count {}", sourceTasks.size());

        syncToDest(sourceTasks);
    }

    /**
     * 将原集群上除了 consumer 外的服务同步到目标集群
     *
     * @param sourceTasks 原集群服务信息
     */
    private void syncToDest(ArrayList<TaskDO> sourceTasks) {
        for (TaskDO sourceTask : sourceTasks) {
            TaskDO old = taskAccessService.findByTaskId(sourceTask.getTaskId());
            if (old == null) {
                taskAccessService.addTask(sourceTask);
            }
        }
        log.info("end syncToDest, add task count {}", sourceTasks.size());
    }

    private String serviceInfo(ClusterDO clusterDO) {
        List<String> hosts = skyWalkerCacheServices.getAllClusterConnectKey(clusterDO.getClusterId());
        if (ObjectUtils.isEmpty(hosts)) {
            log.error("hosts is empty {}", clusterDO);
            return null;
        }
        String urlPath = "/nacos/v1/ns/catalog/services?hasIpCount=true&pageNo=1&pageSize=1000000" +
                "&serviceNameParam=&groupNameParam=&namespaceId=" + clusterDO.getNamespace() + "&withInstances=false";
        String result = httpService.httpGet("http://" + hosts.get(0) + ":6802" + urlPath);
        if (ObjectUtils.isEmpty(result)) {
            return null;
        }
        try {
            return result;
        } catch (Exception e) {
            log.error("error while parse json");
        }
        return null;
    }

    /**
     * 解析 json，返回服务名称，ip list，key 为服务名称.
     */
    private ArrayList<TaskDO> servicesToTask(String json, String sourceClusterId, String destClusterId) throws Exception {

        ArrayList<TaskDO> tasks = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = (ObjectNode) objectMapper.readTree(json);
        ArrayNode arrayNode = (ArrayNode) objectNode.get("serviceList");

        for (JsonNode jsonNode : arrayNode) {
            String serviceName = jsonNode.get("name").asText();
            int ipCount = jsonNode.get("ipCount").asInt();
            if (ObjectUtils.isEmpty(serviceName)) {
                String error = jsonNode.toPrettyString();
                throw new RuntimeException("serviceName is empty: " + error);
            }

            if (ipCount < 1) {
                log.warn("ipCount less than 1, serviceName is {}", serviceName);
            }

            if (serviceName.startsWith("consumers:")) {
                continue;
            }

            String groupName = jsonNode.get("groupName").asText();

            TaskDO taskDO = generateTask(serviceName, groupName, sourceClusterId, destClusterId);
            taskDO.setIpCount(ipCount);
            tasks.add(taskDO);
        }
        return tasks;
    }

    private TaskDO generateTask(String serviceName, String groupName, String sourceClusterId, String destClusterId) throws Exception {
        String taskId = SkyWalkerUtil.generateTaskId(serviceName, groupName, sourceClusterId, destClusterId);
        TaskDO taskDO = taskAccessService.findByTaskId(taskId);
        if (taskDO == null) {
            taskDO = new TaskDO();
            taskDO.setServiceName(serviceName);
            taskDO.setGroupName(groupName);
            taskDO.setTaskId(taskId);
            taskDO.setSourceClusterId(sourceClusterId);
            taskDO.setDestClusterId(destClusterId);
            taskDO.setVersion("");
            taskDO.setNameSpace("");
            taskDO.setWorkerIp(SkyWalkerUtil.getLocalIp());
        }
        taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
        taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
        return taskDO;
    }
}
