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

        beginAsyncToNacos("fs-test-01", "fs-loong-01");
        beginAsyncToNacos("fs-test-01-test", "fs-loong-01-test");
        beginAsyncToNacos("fs-test-01-deve", "fs-loong-01-deve");
        beginAsyncToNacos("fs-test-01-local", "fs-loong-01-local");
        beginAsyncToNacos("fs-test-01-press", "fs-loong-01-press");
        beginAsyncToNacos("fs-test-0-sandbox1", "fs-loong-01-sandbox");
        beginAsyncToNacos("fs-test-01-sl-ecom-old-test", "fs-loong-01-sl-ecom-old-test");
        beginAsyncToNacos("fs-test-01-sl-ecom-new-test", "fs-loong-01-sl-ecom-new-test");
        beginAsyncToNacos("fs-test-01-sl-ecom-new-dev", "fs-loong-01-sl-ecom-new-dev");
        beginAsyncToNacos("fs-test-01-tester-test", "fs-loong-01-tester-test");
        beginAsyncToNacos("fs-test-01-sl-jdp", "fs-loong-01-sl-jdp");
        beginAsyncToNacos("fs-test-01-data_platform", "fs-loong-01-data_platform");
        beginAsyncToNacos("fs-test-01-sl-ecom-shopify-test", "fs-loong-01-sl-ecom-shopify-test");
        beginAsyncToNacos("fs-test-01-ecom-others-test", "fs-loong-01-ecom-others-test");
        beginAsyncToNacos("fs-test-01-slp-local", "fs-loong-01-slp-local");
        beginAsyncToNacos("fs-test-01-slp-test", "fs-loong-01-slp-test");
        beginAsyncToNacos("fs-test-01-slp-develop", "fs-loong-01-slp-develop");
        beginAsyncToNacos("fs-test-01-ecom-open-test", "fs-loong-01-ecom-open-test");
        beginAsyncToNacos("fs-test-01-slp-sandbox", "fs-loong-01-slp-sandbox");
        beginAsyncToNacos("fs-test-01-sale-press", "fs-loong-01-sale-press");
        beginAsyncToNacos("fs-test-01-sales-af-press", "fs-loong-01-sales-af-press");
        beginAsyncToNacos("fs-test-01-product-press", "fs-loong-01-product-press");
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
        addCluster("fs-test-01", "10.98.64.84:6801", "");
        addCluster("fs-test-01-test", "10.98.64.84:6801", "test");
        addCluster("fs-test-01-deve", "10.98.64.84:6801", "develop");
        addCluster("fs-test-01-local", "10.98.64.84:6801", "local");
        addCluster("fs-test-01-press", "10.98.64.84:6801", "press");
        addCluster("fs-test-01-sandbox", "10.98.64.84:6801", "sandbox");
        addCluster("fs-test-01-sl-ecom-old-test", "10.98.64.84:6801",
                "68364d5e-c6e0-4012-b6d2-c9106a688932");
        addCluster("fs-test-01-sl-ecom-new-test", "10.98.64.84:6801",
                "21ffcfab-d33d-4764-97ad-94de7f64493d");
        addCluster("fs-test-01-sl-ecom-new-dev", "10.98.64.84:6801",
                "4636c623-93b6-472b-8d48-f482aed74c98");
        addCluster("fs-test-01-tester-test", "10.98.64.84:6801", "tester-test");
        addCluster("fs-test-01-sl-jdp", "10.98.64.84:6801", "sl-jdp");
        addCluster("fs-test-01-data_platform", "10.98.64.84:6801", "data_platform");
        addCluster("fs-test-01-sl-ecom-shopify-test", "10.98.64.84:6801", "7758344d-2c6b-487e-8eed-c78b97fc1f1f");
        addCluster("fs-test-01-sl-ecom-others-test", "10.98.64.84:6801", "bcdbf3ec-b0f4-4d8e-9156-36a3c19dc2e2");
        addCluster("fs-test-01-slp-local", "10.98.64.84:6801", "slp-local");
        addCluster("fs-test-01-slp-test", "10.98.64.84:6801", "slp-test");
        addCluster("fs-test-01-slp-develop", "10.98.64.84:6801", "slp-develop");
        addCluster("fs-test-01-sl-ecom-open-test", "10.98.64.84:6801", "8209e0ba-d4d7-41a0-b8e3-4c96e2faf15e");
        addCluster("fs-test-01-slp-sandbox", "10.98.64.84:6801", "slp-sandbox");
        addCluster("fs-test-01-sale-press", "10.98.64.84:6801", "sale-press");
        addCluster("fs-test-01-sales-af-press", "10.98.64.84:6801", "sales-af-press");
        addCluster("fs-test-01-product-press", "10.98.64.84:6801", "product-press");

        addCluster("fs-loong-01", "10.98.64.94:6801", "");
        addCluster("fs-loong-01-test", "10.98.64.94:6801", "test");
        addCluster("fs-loong-01-deve", "10.98.64.94:6801", "develop");
        addCluster("fs-loong-01-local", "10.98.64.94:6801", "local");
        addCluster("fs-loong-01-press", "10.98.64.94:6801", "press");
        addCluster("fs-loong-01-sandbox", "10.98.64.94:6801", "sandbox");
        addCluster("fs-loong-01-sl-ecom-old-test", "10.98.64.94:6801",
                "68364d5e-c6e0-4012-b6d2-c9106a688932");
        addCluster("fs-loong-01-sl-ecom-new-test", "10.98.64.94:6801",
                "21ffcfab-d33d-4764-97ad-94de7f64493d");
        addCluster("fs-loong-01-sl-ecom-new-dev", "10.98.64.94:6801",
                "4636c623-93b6-472b-8d48-f482aed74c98");
        addCluster("fs-loong-01-tester-test", "10.98.64.94:6801", "tester-test");
        addCluster("fs-loong-01-sl-jdp", "10.98.64.94:6801", "sl-jdp");
        addCluster("fs-loong-01-data_platform", "10.98.64.94:6801", "data_platform");
        addCluster("fs-loong-01-sl-ecom-shopify-test", "10.98.64.94:6801", "7758344d-2c6b-487e-8eed-c78b97fc1f1f");
        addCluster("fs-loong-01-sl-ecom-others-test", "10.98.64.94:6801", "bcdbf3ec-b0f4-4d8e-9156-36a3c19dc2e2");
        addCluster("fs-loong-01-slp-local", "10.98.64.94:6801", "slp-local");
        addCluster("fs-loong-01-slp-test", "10.98.64.94:6801", "slp-test");
        addCluster("fs-loong-01-slp-develop", "10.98.64.94:6801", "slp-develop");
        addCluster("fs-loong-01-sl-ecom-open-test", "10.98.64.94:6801", "8209e0ba-d4d7-41a0-b8e3-4c96e2faf15e");
        addCluster("fs-loong-01-slp-sandbox", "10.98.64.94:6801", "slp-sandbox");
        addCluster("fs-loong-01-sale-press", "10.98.64.94:6801", "sale-press");
        addCluster("fs-loong-01-sales-af-press", "10.98.64.94:6801", "sales-af-press");
        addCluster("fs-loong-01-product-press", "10.98.64.94:6801", "product-press");
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
