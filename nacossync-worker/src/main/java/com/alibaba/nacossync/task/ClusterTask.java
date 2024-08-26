package com.alibaba.nacossync.task;

import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.service.ToolsService;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 杨春 At 2022-06-23 16:08
 */

@Slf4j
@Component
public class ClusterTask implements CommandLineRunner {

    private final ClusterAccessService clusterAccessService;
    private final ObjectMapper objectMapper;
    private final ToolsService toolsService;

    public ClusterTask(ClusterAccessService clusterAccessService, ObjectMapper objectMapper, ToolsService toolsService) {
        this.clusterAccessService = clusterAccessService;
        this.objectMapper = objectMapper;
        this.toolsService = toolsService;
    }

    @Override
    public void run(String... args) throws Exception {
        addAllCluster();

        beginAsyncToNacos("nacos-fjny-prev", "loong-fjny-prev");
        beginAsyncToNacos("nacos-fjny-prod", "loong-fjny-prod");
        beginAsyncToNacos("nacos-fjny-ai_prev", "loong-fjny-ai_prev");
        beginAsyncToNacos("nacos-fjny-ai_prod", "loong-fjny-ai_prod");
        beginAsyncToNacos("nacos-fjny-slp_prev", "loong-fjny-slp_prev");
    }

    private void beginAsyncToNacos(String sourceClusterName, String destClusterName) {
        beginAsync(sourceClusterName, destClusterName);
        beginAsync(destClusterName, sourceClusterName);
    }

    private void beginAsync(String sourceClusterName, String destClusterName) {
        Page<ClusterDO> page = clusterAccessService.findPageNoCriteria(0, 100);
        List<ClusterDO> list = page.getContent();

        ClusterDO sourceCluster = null;
        ClusterDO destCluster = null;
        for (ClusterDO clusterDO : list) {
            if (clusterDO.getClusterName().equals(sourceClusterName)) {
                sourceCluster = clusterDO;
            }
            if (clusterDO.getClusterName().equals(destClusterName)) {
                destCluster = clusterDO;
            }
        }

        if (sourceCluster == null) {
            log.error("sourceCluster is null return");
            return;
        }

        if (destCluster == null) {
            log.error("destCluster is null return");
            return;
        }

        toolsService.tryToStartAsync(sourceCluster.getClusterId(), destCluster.getClusterId());
    }

    private void addAllCluster() {
        addCluster("nacos-fjny-prev", "10.92.208.253", "preview");
        addCluster("nacos-fjny-prod", "10.92.209.20", "product");
        addCluster("nacos-fjny-ai_prev", "10.92.209.20", "ai_preview");
        addCluster("nacos-fjny-ai_prod", "10.92.209.20", "ai_product");
        addCluster("nacos-fjny-slp_prev", "10.92.209.20", "slp-preview");

        addCluster("loong-fjny-prev", "10.92.210.181", "preview");
        addCluster("loong-fjny-prod", "10.84.22.69", "product");
        addCluster("loong-fjny-ai_prev", "10.84.22.69", "ai_preview");
        addCluster("loong-fjny-ai_prod", "10.84.22.69", "ai_product");
        addCluster("loong-fjny-slp_prev", "10.84.22.69", "slp-preview");
    }

    private void addCluster(String name, String address, String namespace) {
        try {
            ClusterAddRequest clusterAddRequest = new ClusterAddRequest();
            clusterAddRequest.setClusterName(name);
            clusterAddRequest.setNamespace(namespace);
            clusterAddRequest.setConnectKeyList(new ArrayList<>());
            clusterAddRequest.getConnectKeyList().add(address);
            clusterAddRequest.setClusterType("NACOS");
            addCluster(clusterAddRequest);
        } catch (Exception e) {
            log.error("error while add cluster", e);
        }
    }

    private void addCluster(ClusterAddRequest clusterAddRequest) throws Exception {
        String clusterId = SkyWalkerUtil.generateClusterId(clusterAddRequest);

        if (null != clusterAccessService.findByClusterId(clusterId)) {

            throw new SkyWalkerException("重复插入，clusterId已存在：" + clusterId);
        }

        ClusterDO clusterDO = new ClusterDO();
        clusterDO.setClusterId(clusterId);
        clusterDO.setClusterName(clusterAddRequest.getClusterName());
        clusterDO.setClusterType(clusterAddRequest.getClusterType());
        clusterDO.setConnectKeyList(objectMapper.writeValueAsString(clusterAddRequest.getConnectKeyList()));
        clusterDO.setUserName(clusterAddRequest.getUserName());
        clusterDO.setPassword(clusterAddRequest.getPassword());
        clusterDO.setNamespace(clusterAddRequest.getNamespace());
        if ("develop".equals(clusterAddRequest.getNamespace()) || "test".equals(clusterAddRequest.getNamespace())) {
            clusterDO.setHeartbeatThreads(32);
            clusterDO.setPullThreadCount(16);
        } else {
            clusterDO.setHeartbeatThreads(4);
            clusterDO.setPullThreadCount(2);
        }
        clusterAccessService.insert(clusterDO);
    }
}
