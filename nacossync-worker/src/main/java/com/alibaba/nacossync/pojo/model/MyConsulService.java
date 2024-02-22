package com.alibaba.nacossync.pojo.model;

import com.ecwid.consul.v1.agent.model.NewService;
import lombok.Getter;
import lombok.Setter;

/**
 * @author 杨春 At 2024-02-22 15:51
 */

@Getter
@Setter
public class MyConsulService extends NewService {

    private String node;
}
