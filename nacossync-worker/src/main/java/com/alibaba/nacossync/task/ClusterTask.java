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

        beginAsyncToNacos("nacos-xjp", "loong-xjp");
        beginAsyncToNacos("nacos-xjp_prod", "loong-xjp_prod");
        beginAsyncToNacos("nacos-xjp_ecom_old", "loong-xjp_ecom_old");
        beginAsyncToNacos("nacos-xjp_ecom_new", "loong-xjp_ecom_new");
        beginAsyncToNacos("nacos-xjp_ecom_new_prev", "loong-xjp_ecom_new_prev");
        beginAsyncToNacos("nacos-xjp_sl_jdp", "loong-xjp_sl_jdp");
        beginAsyncToNacos("nacos-xjp_data_platform", "loong-xjp_data_platform");
        beginAsyncToNacos("nacos-xjp_ecom_sf", "loong-xjp_ecom_sf");
        beginAsyncToNacos("nacos-xjp_ecom_ot", "loong-xjp_ecom_ot");
        beginAsyncToNacos("nacos-xjp_slp_product", "loong-xjp_slp_product");
        beginAsyncToNacos("nacos-xjp_ecom_webhook", "loong-xjp_ecom_webhook");
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
        addCluster("nacos-xjp", "10.90.209.207", "");
        addCluster("nacos-xjp_prod", "10.90.210.254", "product");
        addCluster("nacos-xjp_ecom_old", "10.90.216.208", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("nacos-xjp_ecom_new", "10.90.216.208", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("nacos-xjp_ecom_new_prev", "10.90.216.208", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("nacos-xjp_sl_jdp", "10.90.216.208", "sl-jdp");
        addCluster("nacos-xjp_data_platform", "10.90.216.208", "data_platform");
        addCluster("nacos-xjp_ecom_sf", "10.90.216.208", "sl-ecom-sf-prod");
        addCluster("nacos-xjp_ecom_ot", "10.90.216.208", "sl-ecom-ot-prod");
        addCluster("nacos-xjp_slp_product", "10.90.216.208", "slp-product");
        addCluster("nacos-xjp_ecom_webhook", "10.90.216.208", "sl-ecom-new-prod-webhook");

        addCluster("loong-xjp", "10.90.208.85", "");
        addCluster("loong-xjp_prod", "10.90.209.73", "product");
        addCluster("loong-xjp_ecom_old", "10.90.210.32", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("loong-xjp_ecom_new", "10.90.210.32", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("loong-xjp_ecom_new_prev", "10.90.210.32", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("loong-xjp_sl_jdp", "10.90.210.32", "sl-jdp");
        addCluster("loong-xjp_data_platform", "10.90.210.32", "data_platform");
        addCluster("loong-xjp_ecom_sf", "10.90.210.32", "sl-ecom-sf-prod");
        addCluster("loong-xjp_ecom_ot", "10.90.210.32", "sl-ecom-ot-prod");
        addCluster("loong-xjp_slp_product", "10.90.210.32", "slp-product");
        addCluster("loong-xjp_ecom_webhook", "10.90.210.32", "sl-ecom-new-prod-webhook");
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
