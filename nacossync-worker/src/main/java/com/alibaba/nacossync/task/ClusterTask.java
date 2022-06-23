package com.alibaba.nacossync.task;

import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

/**
 * @author 杨春 At 2022-06-23 16:08
 */

@Slf4j
@Component
public class ClusterTask implements CommandLineRunner {

    private final ClusterAccessService clusterAccessService;
    private final ObjectMapper objectMapper;

    @Value("${sl.nacos.sourceCluster}")
    private String sourceCluster;

    @Value("${sl.nacos.destCluster}")
    private String destCluster;

    public ClusterTask(ClusterAccessService clusterAccessService, ObjectMapper objectMapper) {
        this.clusterAccessService = clusterAccessService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        addCluster("fs-test", "ehlxm.nacos.duowan.com:6801", "develop");
        addCluster("nacos-pod", "10.218.18.224:6802", "develop");

        addCluster("xjp-preview", "shopline-nacos-xjp-prod-87acd84ebecadb93.elb.ap-southeast-1.amazonaws.com:6802", "preview");
        addCluster("xjp-product", "shopline-nacos-xjp-prod-87acd84ebecadb93.elb.ap-southeast-1.amazonaws.com:6802", "product");

        addCluster("fjny-preview", "shopline-nacos-fjny-prod-ebadfe4360977950.elb.us-east-1.amazonaws.com:6802", "preview");
        addCluster("fjny-product", "shopline-nacos-fjny-prod-ebadfe4360977950.elb.us-east-1.amazonaws.com:6802", "product");

        addCluster("xjp-v3-preview", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com:6802", "preview");
        addCluster("xjp-v3-product", "nacos-xjp-v3-0-2f27655bf16702c8.elb.ap-southeast-1.amazonaws.com:6802", "product");

        addCluster("fjny-v3-preview", "nacos-fjny-v3-0-27a9de3b376a8cd7.elb.us-east-1.amazonaws.com:6802", "preview");
        addCluster("fjny-v3-product", "nacos-fjny-v3-0-27a9de3b376a8cd7.elb.us-east-1.amazonaws.com:6802", "product");
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
