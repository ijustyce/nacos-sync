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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final AtomicBoolean started = new AtomicBoolean(false);

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
        if (started.compareAndSet(false, true)) {
            log.info("started is {} try to start now.", started.get());
            runNacosServiceAsync(sourceClusterId, destClusterId);
            return;
        }
        log.error("started is {} ignore.", started.get());
    }

    private void runNacosServiceAsync(String sourceClusterId, String destClusterId) {
        log.info("begin runNacosServiceAsync");
        scheduledService.execute(() -> {
            try {
                asyncNacosServices(sourceClusterId, destClusterId);
            } catch (Exception e) {
                log.error("error while async nacos service", e);
            }
        });

        scheduledService.schedule(() -> runNacosServiceAsync(sourceClusterId, destClusterId), 5, TimeUnit.SECONDS);
        log.info("end runNacosServiceAsync");
    }

    private void asyncNacosServices(String sourceClusterId, String destClusterId) throws Exception {
        log.info("begin asyncNacosServices");

        ClusterDO sourceCluster = clusterAccessService.findByClusterId(sourceClusterId);
        ClusterDO destCluster = clusterAccessService.findByClusterId(destClusterId);

        String sourceServices = serviceInfo(sourceCluster);
        String destServices = serviceInfo(destCluster);

        ArrayList<TaskDO> sourceTasks = servicesToTask(sourceServices, sourceClusterId, destClusterId);
        ArrayList<TaskDO> destTasks = servicesToTask(destServices, sourceClusterId, destClusterId);

        ArrayList<TaskDO> toAddTasks = diffTasks(sourceTasks, destTasks);
        if (ObjectUtils.isEmpty(toAddTasks)) {
            log.info("end asyncNacosServices, no task added.");
            return;
        }

        toAddTasks.forEach(taskAccessService::addTask);
        log.info("end asyncNacosServices, add task count {}", toAddTasks.size());
    }

    private ArrayList<TaskDO> diffTasks(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        if (ObjectUtils.isEmpty(sourceTasks)) {
            return null;
        }

        if (ObjectUtils.isEmpty(destTasks)) {
            return sourceTasks;
        }

        ArrayList<TaskDO> result = new ArrayList<>();
        for (TaskDO task : sourceTasks) {
            String serviceName = task.getServiceName();
            if (ObjectUtils.isEmpty(serviceName)) {
                continue;
            }
            if (serviceName.startsWith("consumers:")) {
                continue;
            }
            boolean isExists = isExists(destTasks, serviceName);
            if (!isExists) {
                result.add(task);
            }
        }
        return result;
    }

    private boolean isExists(ArrayList<TaskDO> destTasks, String servicesName) {
        for (TaskDO taskDO : destTasks) {
            if (servicesName.equals(taskDO.getServiceName())) {
                return true;
            }
        }
        return false;
    }

    private String serviceInfo(ClusterDO clusterDO) {
        List<String> hosts = skyWalkerCacheServices.getAllClusterConnectKey(clusterDO.getClusterId());
        if (ObjectUtils.isEmpty(hosts)) {
            log.error("hosts is empty {}", clusterDO);
            return null;
        }
        String urlPath = "/nacos/v1/ns/catalog/services?hasIpCount=true&withInstances=false&pageNo=1&pageSize=1000000" +
                "&serviceNameParam=&groupNameParam=&namespaceId=" + clusterDO.getNamespace();
        String result = httpService.httpGet(hosts.get(0) + urlPath);
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
        ArrayNode arrayNode = (ArrayNode) objectMapper.readTree(json);

        for (JsonNode jsonNode : arrayNode) {
            String serviceName = jsonNode.get("serviceName").asText();
            String groupName = jsonNode.get("groupName").asText();
            if (ObjectUtils.isEmpty(serviceName)) {
                String error = jsonNode.toPrettyString();
                throw new RuntimeException("serviceName is empty: " + error);
            }

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
                taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
                taskDO.setWorkerIp(SkyWalkerUtil.getLocalIp());
                taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
            } else {
                taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
                taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
            }
            tasks.add(taskDO);
        }
        return tasks;
    }
}
