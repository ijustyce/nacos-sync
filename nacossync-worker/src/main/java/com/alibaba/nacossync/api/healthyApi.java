package com.alibaba.nacossync.api;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.FinishedTask;
import com.alibaba.nacossync.pojo.model.TaskDO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author 杨春 At 2024-07-17 10:06
 */

@RequestMapping("/api/healthy")
@RestController
public class healthyApi {

    private final SkyWalkerCacheServices services;

    private final Iterable<TaskDO> taskDOS;

    private final AtomicBoolean isReady = new AtomicBoolean(false);

    public healthyApi(SkyWalkerCacheServices services, TaskAccessService taskAccessService) {
        this.services = services;
        this.taskDOS = taskAccessService.findAll();
    }

    @GetMapping("/ready")
    public void ready() {
        if (isReady.get()) {
            return;
        }

        for (TaskDO taskDO : taskDOS) {
            FinishedTask finishedTask = services.getFinishedTask(taskDO);
            if (finishedTask == null) {
                throw new RuntimeException("task not finished");
            }
        }

        isReady.set(true);
    }

    @GetMapping("/preStop")
    public void preStop() {
        try {
            //  休眠 180s
            Thread.sleep(180_000);
        }catch (Exception ignore){}
    }
}
