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
        beginAsync(destClusterName, sourceClusterName);
        beginAsync(sourceClusterName, destClusterName);
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
        addCluster("nacos-xjp", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "");
        addCluster("nacos-xjp_prod", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "product");
        addCluster("nacos-xjp_ecom_old", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("nacos-xjp_ecom_new", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("nacos-xjp_ecom_new_prev", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("nacos-xjp_sl_jdp", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "sl-jdp");
        addCluster("nacos-xjp_data_platform", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "data_platform");
        addCluster("nacos-xjp_ecom_sf", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "sl-ecom-sf-prod");
        addCluster("nacos-xjp_ecom_ot", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "sl-ecom-ot-prod");
        addCluster("nacos-xjp_slp_product", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "slp-product");
        addCluster("nacos-xjp_ecom_webhook", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com", "sl-ecom-new-prod-webhook");

        addCluster("loong-xjp", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "");
        addCluster("loong-xjp_prod", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "product");
        addCluster("loong-xjp_ecom_old", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "d5d75bd7-935e-4057-af25-126a946b321f");
        addCluster("loong-xjp_ecom_new", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "7164b7c1-28f0-4d87-9ed2-6a30bdd706cc");
        addCluster("loong-xjp_ecom_new_prev", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("loong-xjp_sl_jdp", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "sl-jdp");
        addCluster("loong-xjp_data_platform", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "data_platform");
        addCluster("loong-xjp_ecom_sf", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "sl-ecom-sf-prod");
        addCluster("loong-xjp_ecom_ot", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "sl-ecom-ot-prod");
        addCluster("loong-xjp_slp_product", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "slp-product");
        addCluster("loong-xjp_ecom_webhook", "k8s-shopline-huloongn-2511f5033a-256c0c292865c47a.elb.ap-southeast-1.amazonaws.com", "sl-ecom-new-prod-webhook");
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
