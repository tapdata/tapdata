package com.tapdata.tm.task.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import com.tapdata.tm.commons.task.dto.TaskDto;

@Data
public class TaskDag {
    // 任务节点信息
    private Map<String, TaskNode> nodes;
    // 任务执行顺序（按层次分组，每层可并行执行）
    private List<Set<String>> parallelExecutionGroups;
    // 任务依赖关系图
    private Map<String, List<TaskDependency>> dependencies;
    
    public TaskDag() {
        this.nodes = new HashMap<>();
        this.parallelExecutionGroups = new ArrayList<>();
        this.dependencies = new HashMap<>();
    }
    
    @Data
    public static class TaskNode {
        private String taskName;
        private String taskId;
        private int inDegree;
        private TaskDto taskDto;
        
        public TaskNode(String taskName, String taskId, TaskDto taskDto) {
            this.taskName = taskName;
            this.taskId = taskId;
            this.inDegree = 0;
            this.taskDto = taskDto;
        }
    }
    
    @Data
    public static class TaskDependency {
        private String taskName;
        private String dependType;
        private String dependEvent;
        
        public TaskDependency(String taskName, String dependType, String dependEvent) {
            this.taskName = taskName;
            this.dependType = dependType;
            this.dependEvent = dependEvent;
        }
    }
} 