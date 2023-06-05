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
//        addCluster("xjp-v3", "nacos-xjp.inshopline.com:6802", "preview");
//        addCluster("xjp-pre", "nacos-xjp-prev.inshopline.com:6802", "preview");
//
//        addCluster("xjp-product", "nacos-xjp.inshopline.com:6802", "product");
//        addCluster("xjp-pre-product", "nacos-xjp-prev.inshopline.com:6802", "product");
//
//        addCluster("xjp-v3-eom", "nacos-xjp.inshopline.com:6802", "2bc5e976-cdb3-4fe4-a781-93de2367c72d");
//        addCluster("xjp-eom", "nacos-xjp-prev.inshopline.com:6802", "2bc5e976-cdb3-4fe4-a781-93de2367c72d");
//
//        addCluster("xjp-v3-eom-new", "nacos-xjp.inshopline.com:6802", "f0082435-ea66-4662-aead-6935e0d5bd9c");
//        addCluster("xjp-eom-new", "nacos-xjp-prev.inshopline.com:6802", "f0082435-ea66-4662-aead-6935e0d5bd9c");
//
//        addCluster("xjp-v3-ai", "nacos-xjp.inshopline.com:6802", "ai_preview");
//        addCluster("xjp-ai", "nacos-xjp-prev.inshopline.com:6802", "ai_preview");

//        beginAsync("xjp-v3", "xjp-pre");

//        beginAsync("xjp-v3-eom", "xjp-eom");

//        beginAsync("xjp-v3-eom-new", "xjp-eom-new");

//        beginAsync("xjp-v3-ai", "xjp-ai");

//        beginAsync("xjp-pre-product", "xjp-product");

        addCluster("fjny-prod-ai", "nacos-fjny.inshopline.com:6802", "ai_preview");
        addCluster("fjny-prev-ai", "nacos-fjny-prev.inshopline.com:6802", "ai_preview");

        addCluster("fjny-prod-prev", "nacos-fjny.inshopline.com:6802", "preview");
        addCluster("fjny-prev-prev", "nacos-fjny-prev.inshopline.com:6802", "preview");

//        beginAsync("fjny-prod-ai", "fjny-prev-ai");
//        beginAsync("fjny-prod-prev", "fjny-prev-prev");
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
