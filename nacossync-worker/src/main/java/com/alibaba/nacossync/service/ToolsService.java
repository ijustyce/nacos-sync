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
import java.util.Iterator;
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
    private final AtomicBoolean withInstance = new AtomicBoolean(true);

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

        log.info("sources task count {}, destTasks count {}", sourceTasks.size(), destTasks.size());
        if (!ObjectUtils.isEmpty(sourceTasks) && !ObjectUtils.isEmpty(destTasks)) {
            withInstance.set(false);
        }

        syncToDest(sourceTasks, destTasks);
        syncIfUpdate(sourceTasks, destTasks);
        syncToSource(sourceTasks, destTasks);
    }

    /**
     * 将 consumer 在原集群，但是 provider 不在原集群而是在目标集群的，将目标集群的 provider 同步到原集群
     */
    private void syncToSource(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        ArrayList<TaskDO> list = diffDestProvider(sourceTasks, destTasks);
        if (ObjectUtils.isEmpty(list)) {
            return;
        }

        list.forEach(this.taskAccessService::addTask);
    }

    /**
     * 查找在 目标集群，但不在原集群的 非 consumer 服务！
     *
     * @param sourceTasks 原集群任务
     * @param destTasks   目标集群任务
     * @return 在目标集群，但不在原集群的 非 consumer 服务
     */
    private ArrayList<TaskDO> diffDestProvider(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        if (ObjectUtils.isEmpty(sourceTasks)) {
            return null;
        }
        if (ObjectUtils.isEmpty(destTasks)) {
            return null;
        }

        ArrayList<TaskDO> result = new ArrayList<>();
        for (TaskDO taskDO : destTasks) {
            //  如果是 consumer 则跳过
            if (taskDO.getServiceName().startsWith("consumers:")) {
                continue;
            }

            TaskDO provider = findByName(sourceTasks, taskDO.getServiceName());
            //  如果原集群存在该 provider 则跳过
            if (provider != null) {
                continue;
            }

            TaskDO toAddTask = syncToSourceTask(taskDO);
            if (toAddTask != null) {
                result.add(toAddTask);
            }
        }
        return result;
    }

    /**
     * 将更新后的服务同步到源集群，解决源集群的 provider 更新后连接到了目标集群，
     * 但是源集群的 consumer 连接的依旧是源集群，导致找不到该 provider
     *
     * @param sourceTasks 原集群服务信息
     * @param destTasks   目标集群服务信息
     */
    private void syncIfUpdate(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        if (ObjectUtils.isEmpty(destTasks)) {
            return;
        }
        //  如果为第一次，则不需要考虑更新的情况
        if (withInstance.get()) {
            return;
        }
        String sourceNacosId = destTasks.get(0).getSourceClusterId();
        String destNacosId = destTasks.get(0).getDestClusterId();

        List<TaskDO> result = new ArrayList<>();

        Iterable<TaskDO> allTask = this.taskAccessService.findAll();
        allTask.forEach(it -> {
            //  只查找同步到目标集群的，反向同步的过滤掉
            if (it.getSourceClusterId().equals(sourceNacosId) && it.getDestClusterId().equals(destNacosId)) {
                TaskDO sourceTask = findByName(sourceTasks, it.getServiceName());
                if (sourceTask == null) {
                    TaskDO tmp = syncToSourceTask(it);
                    if (tmp != null) {
                        result.add(tmp);
                    }
                } else {
                    TaskDO destTask = findByName(destTasks, it.getServiceName());
                    if (destTask != null && destTask.getIpCount() > sourceTask.getIpCount()
                            && destTask.getIpCount() > 0 && sourceTask.getIpCount() > 0) {
                        TaskDO tmp = syncToSourceTask(it);
                        if (tmp != null) {
                            result.add(tmp);
                        }
                    }
                }
            }
        });

        result.forEach(this.taskAccessService::addTask);
    }

    /**
     * 将本来要同步到目标集群的服务，同步到源集群，解决 provider 更新后，连接到新集群时，原集群 consumer 找不到 provider 的情况
     *
     * @param taskDO 原同步任务
     * @return 反向同步的任务
     */
    private TaskDO syncToSourceTask(TaskDO taskDO) {
        if (ObjectUtils.isEmpty(taskDO)) {
            return null;
        }
        //  注意，generateTaskId 函数中的 sourceClusterId 为 taskDO 中的 destClusterId
        String sourceClusterId = taskDO.getDestClusterId();
        String destClusterId = taskDO.getSourceClusterId();

        try {
            return generateTask(taskDO.getServiceName(), taskDO.getGroupName(), sourceClusterId, destClusterId);
        } catch (Exception e) {
            log.error("error while generate task", e);
        }
        return null;
    }

    /**
     * 将原集群上除了 consumer 外的服务同步到目标集群
     *
     * @param sourceTasks 原集群服务信息
     * @param destTasks   目标集群服务信息
     */
    private void syncToDest(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        ArrayList<TaskDO> toAddTasks = diffTasks(sourceTasks, destTasks);
        if (ObjectUtils.isEmpty(toAddTasks)) {
            log.info("end syncToDest, no task added.");
            return;
        }

        toAddTasks.forEach(taskAccessService::addTask);
        log.info("end syncToDest, add task count {}", toAddTasks.size());
    }

    private ArrayList<TaskDO> diffTasks(ArrayList<TaskDO> sourceTasks, ArrayList<TaskDO> destTasks) {
        if (ObjectUtils.isEmpty(sourceTasks)) {
            return null;
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
            //  如果目标集群不存在该服务则添加！
            TaskDO destTask = findByName(destTasks, serviceName);
            if (destTask == null) {
                result.add(task);
            }
        }
        return result;
    }

    private TaskDO findByName(ArrayList<TaskDO> taskList, String servicesName) {
        if (ObjectUtils.isEmpty(taskList)) {
            return null;
        }
        for (TaskDO taskDO : taskList) {
            if (servicesName.equals(taskDO.getServiceName())) {
                return taskDO;
            }
        }
        return null;
    }

    private String serviceInfo(ClusterDO clusterDO) {
        List<String> hosts = skyWalkerCacheServices.getAllClusterConnectKey(clusterDO.getClusterId());
        if (ObjectUtils.isEmpty(hosts)) {
            log.error("hosts is empty {}", clusterDO);
            return null;
        }
        String urlPath = "/nacos/v1/ns/catalog/services?hasIpCount=true&pageNo=1&pageSize=1000000" +
                "&serviceNameParam=&groupNameParam=&namespaceId=" + clusterDO.getNamespace() + "&withInstances="
                + withInstance.get();
        String result = httpService.httpGet("http://" + hosts.get(0) + urlPath);
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
        ArrayNode arrayNode;
        if (withInstance.get()) {
            arrayNode = (ArrayNode) objectMapper.readTree(json);
        } else {
            ObjectNode objectNode = (ObjectNode) objectMapper.readTree(json);
            arrayNode = (ArrayNode) objectNode.get("serviceList");
        }

        for (JsonNode jsonNode : arrayNode) {
            if (isFromSync(jsonNode)) {
                continue;
            }

            String serviceName;
            int ipCount = 0;
            if (jsonNode.has("name")) {
                serviceName = jsonNode.get("name").asText();
                ipCount = jsonNode.get("ipCount").asInt();
            } else {
                serviceName = jsonNode.get("serviceName").asText();
            }
            if (ObjectUtils.isEmpty(serviceName)) {
                String error = jsonNode.toPrettyString();
                throw new RuntimeException("serviceName is empty: " + error);
            }

            String groupName = jsonNode.get("groupName").asText();

            TaskDO taskDO = generateTask(serviceName, groupName, sourceClusterId, destClusterId);
            taskDO.setIpCount(ipCount);
            tasks.add(taskDO);
        }
        return tasks;
    }

    private boolean isFromSync(JsonNode jsonNode) {
        if (!jsonNode.has("clusterMap")) {
            return false;
        }
        JsonNode clusterMap = jsonNode.get("clusterMap");
        Iterator<JsonNode> elements = clusterMap.elements();
        while (elements.hasNext()) {
            JsonNode cluster = elements.next();
            ArrayNode hosts = (ArrayNode) cluster.get("hosts");
            for (JsonNode host : hosts) {
                //  如果是从 nacos 同步的则返回 true
                JsonNode metadata = host.get("metadata");
                if (metadata != null) {
                    JsonNode syncSource = metadata.get("syncSource");
                    if (syncSource != null && "NACOS".equals(syncSource.asText())) {
                        return true;
                    }
                }
            }
        }
        return false;
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
