package com.alibaba.nacossync.api;

import com.alibaba.nacossync.service.ToolsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author 杨春 At 2023-05-16 16:38
 */

@RequestMapping("/tools")
@RestController
public class ToolsApi {

    private final ToolsService toolsService;

    public ToolsApi(ToolsService toolsService) {
        this.toolsService = toolsService;
    }

    @GetMapping("/add")
    public void add(String sourceId, String destId) {
        toolsService.tryToStartAsync(sourceId, destId);
    }
}
