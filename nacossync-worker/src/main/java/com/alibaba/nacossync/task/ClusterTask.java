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
        addCluster("xjp-product", "10.86.169.96:6802", "product");
        addCluster("xjp-eco-old", "10.86.169.96:6802", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("xjp-eco-new", "10.86.169.96:6802", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("xjp-sl-jdp", "10.86.169.96:6802", "sl-jdp");
        addCluster("xjp-data-platform", "10.86.169.96:6802", "data_platform");
        addCluster("xjp-ai-product", "10.86.169.96:6802", "ai_product");
        addCluster("xjp-sf-product", "10.86.169.96:6802", "sl-ecom-sf-prod");
        addCluster("xjp-ot-product", "10.86.169.96:6802", "sl-ecom-ot-prod");

        addCluster("xjp-new-product", "10.90.209.115:6802", "product");
        addCluster("xjp-new-eco-old", "10.90.210.107:6802", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("xjp-new-eco-new", "10.90.210.107:6802", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("xjp-new-sl-jdp", "10.90.210.107:6802", "sl-jdp");
        addCluster("xjp-new-data-platform", "10.90.210.107:6802", "data_platform");
        addCluster("xjp-new-ai-product", "10.90.210.107:6802", "ai_product");
        addCluster("xjp-new-sf-product", "10.90.210.107:6802", "sl-ecom-sf-prod");
        addCluster("xjp-new-ot-product", "10.90.210.107:6802", "sl-ecom-ot-prod");

        beginAsync("xjp-product", "xjp-new-product");
        beginAsync("xjp-eco-old", "xjp-new-eco-old");
        beginAsync("xjp-eco-new", "xjp-new-eco-new");
        beginAsync("xjp-sl-jdp", "xjp-new-sl-jdp");
        beginAsync("xjp-data-platform", "xjp-new-data-platform");
        beginAsync("xjp-ai-product", "xjp-new-ai-product");
        beginAsync("xjp-sf-product", "xjp-new-sf-product");
        beginAsync("xjp-ot-product", "xjp-new-ot-product");
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
        clusterAccessService.insert(clusterDO);
    }
}
