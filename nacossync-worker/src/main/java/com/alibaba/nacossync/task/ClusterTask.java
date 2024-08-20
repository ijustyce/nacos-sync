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
        beginAsyncToNacos("nacos-xjp-prev", "loong-xjp-prev");
        beginAsyncToNacos("nacos-xjp-prev_ecom_old", "loong-xjp-prev_ecom_old");
        beginAsyncToNacos("nacos-xjp-prev_ecom_new", "loong-xjp-prev_ecom_new");
        beginAsyncToNacos("nacos-xjp-prev_sf", "loong-xjp-prev_sf");
        beginAsyncToNacos("nacos-xjp-prev_ot", "loong-xjp-prev_ot");
        beginAsyncToNacos("nacos-xjp-prev_slp", "loong-xjp-prev_slp");
        beginAsyncToNacos("nacos-xjp-prev_self", "loong-xjp-prev_self");
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
        addCluster("nacos-xjp-prev", "10.90.208.85", "preview");
        addCluster("nacos-xjp-prev_ecom_old", "10.90.209.73", "2bc5e976-cdb3-4fe4-a781-93de2367c72d");
        addCluster("nacos-xjp-prev_ecom_new", "10.90.210.32", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("nacos-xjp-prev_sf", "10.90.210.32", "sl-ecom-sf-preview");
        addCluster("nacos-xjp-prev_ot", "10.90.209.73", "sl-ecom-ot-preview");
        addCluster("nacos-xjp-prev_slp", "10.90.209.73", "slp-preview");

        addCluster("loong-xjp-prev", "loong-xjp-preview.inshopline.com", "preview");
        addCluster("loong-xjp-prev_ecom_old", "loong-xjp-preview.inshopline.com", "2bc5e976-cdb3-4fe4-a781-93de2367c72d");
        addCluster("loong-xjp-prev_ecom_new", "loong-xjp-preview.inshopline.com", "f0082435-ea66-4662-aead-6935e0d5bd9c");
        addCluster("loong-xjp-prev_sf", "loong-xjp-preview.inshopline.com", "sl-ecom-sf-preview");
        addCluster("loong-xjp-prev_ot", "loong-xjp-preview.inshopline.com", "sl-ecom-ot-preview");
        addCluster("loong-xjp-prev_slp", "loong-xjp-preview.inshopline.com", "slp-preview");
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
