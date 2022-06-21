//package com.alibaba.nacossync.service;
//
//import com.alibaba.nacossync.constant.TaskStatusEnum;
//import com.alibaba.nacossync.dao.ClusterAccessService;
//import com.alibaba.nacossync.dao.TaskAccessService;
//import com.alibaba.nacossync.pojo.model.TaskDO;
//import com.alibaba.nacossync.util.SkyWalkerUtil;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.stereotype.Service;
//import org.springframework.util.ObjectUtils;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//
///**
// * @author 杨春 At 2022-06-21 15:50
// */
//
//@Slf4j
//@Service
//public class ToolsService {
//
//    private final HttpService httpService;
//    private final TaskAccessService taskAccessService;
//    private final ClusterAccessService clusterAccessService;
//
//    public ToolsService(HttpService httpService, TaskAccessService taskAccessService,
//                        ClusterAccessService clusterAccessService) {
//        this.httpService = httpService;
//        this.taskAccessService = taskAccessService;
//        this.clusterAccessService = clusterAccessService;
//    }
//
//    private ArrayList<TaskDO> fetchNacosService(String sourceClusterId, String destClusterId) {
//        String urlPath = "/nacos/v1/ns/catalog/services?hasIpCount=true&withInstances=false&pageNo=1&pageSize=1000000" +
//                "&serviceNameParam=&groupNameParam=&namespaceId=" + namespace;
//        String result = httpService.httpGet(host + urlPath);
//        if (ObjectUtils.isEmpty(result)) {
//            return null;
//        }
//        try {
//            return parseServices(result);
//        } catch (Exception e) {
//            log.error("error while parse json");
//        }
//        return null;
//    }
//
//    /**
//     * 解析 json，返回服务名称，ip list，key 为服务名称.
//     */
//    private ArrayList<TaskDO> parseServices(String json, String sourceClusterId, String destClusterId) throws Exception {
//
//        ArrayList<TaskDO> tasks = new ArrayList<>();
//        ObjectMapper objectMapper = new ObjectMapper();
//        ArrayNode arrayNode = (ArrayNode) objectMapper.readTree(json);
//
//        for (JsonNode jsonNode : arrayNode) {
//            String serviceName = jsonNode.get("serviceName").asText();
//            String groupName = jsonNode.get("groupName").asText();
//            if (ObjectUtils.isEmpty(serviceName)) {
//                String error = jsonNode.toPrettyString();
//                throw new RuntimeException("serviceName is empty: " + error);
//            }
//
//            String taskId = SkyWalkerUtil.generateTaskId(serviceName, groupName, sourceClusterId, destClusterId);
//            TaskDO taskDO = taskAccessService.findByTaskId(taskId);
//            if (taskDO == null) {
//                taskDO = new TaskDO();
//                taskDO.setServiceName(serviceName);
//                taskDO.setGroupName(groupName);
//                taskDO.setTaskId(taskId);
//                taskDO.setSourceClusterId(sourceClusterId);
//                taskDO.setDestClusterId(destClusterId);
//                taskDO.setVersion();
//                taskDO.setNameSpace();
//                taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
//                taskDO.setWorkerIp(SkyWalkerUtil.getLocalIp());
//                taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
//            }else {
//                taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
//                taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
//            }
//
//            JsonNode clusterMap = jsonNode.get("clusterMap");
//            Iterator<JsonNode> elements = clusterMap.elements();
//            while (elements.hasNext()) {
//                JsonNode cluster = elements.next();
//                ArrayNode hosts = (ArrayNode) cluster.get("hosts");
//                for (JsonNode host : hosts) {
//                    String ip = host.get("ip").asText();
//                    ArrayList<String> tmpIps = serviceMap.get(serviceName);
//                    if (tmpIps == null) {
//                        tmpIps = new ArrayList<>();
//                        tmpIps.add(ip);
//                        serviceMap.put(serviceName, tmpIps);
//                    } else {
//                        tmpIps.add(ip);
//                    }
//                }
//            }
//        }
//        return serviceMap;
//    }
//}
