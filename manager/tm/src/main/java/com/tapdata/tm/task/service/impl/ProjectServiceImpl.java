package com.tapdata.tm.task.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.quartz.CronScheduleBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.impl.TaskDag.TaskDependency;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import lombok.NonNull;
import lombok.Setter;
import org.springframework.transaction.annotation.Transactional;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.ProjectDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.ProjectRepository;
import com.tapdata.tm.task.service.ProjectService;

import lombok.extern.slf4j.Slf4j;

/**
 * @Author: Jerry Gao
 * @Date: 2024/11/22
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ProjectServiceImpl extends ProjectService {

    public static final String IS_DELETED = "isDeleted";

    @Autowired
    private TaskService taskService;
    
    public ProjectServiceImpl(@NonNull ProjectRepository repository) {
        super(repository);
    }
    
    @Override
    public ProjectDto create(ProjectDto projectDto, UserDetail userDetail) {
        projectDto.setStatus(ProjectDto.STATUS_EDIT);

        if (StringUtils.isNotEmpty(projectDto.getCrontabExpression())) {
            try {
                CronScheduleBuilder.cronSchedule(projectDto.getCrontabExpression());
            } catch (Exception e) {
                throw new BizException("Project.CronError");
            }
        }

        checkProjectName(projectDto.getName(), userDetail);
        projectDto = save(projectDto, userDetail);
        
        return projectDto;
    }

    private void checkProjectName(String name, UserDetail userDetail) {
        checkProjectName(name, userDetail, null);
    }

    public void checkProjectName(String name, UserDetail userDetail, ObjectId id) {
        if (StringUtils.isBlank(name)) {
            throw new BizException("Project.NameIsNull");
        }
        if (checkProjectNameNotError(name, userDetail, id)) {
            throw new BizException("Project.RepeatName");
        }
    }

    public boolean checkProjectNameNotError(String name, UserDetail user, ObjectId id) {

        Criteria criteria = Criteria.where("name").is(name).and(IS_DELETED).ne(true);
        if (id != null) {
            criteria.and("_id").ne(id);
        }
        Query query = new Query(criteria);
        long count = count(query, user);
        return count > 0;
    }

    @Override
    public ProjectDto update(ProjectDto projectDto, UserDetail user) {

        // check project id
        if (projectDto.getId() == null) {
            throw new BizException("Project.IdIsNull");
        } else if (findById(projectDto.getId()) == null) {
            log.debug("project not found, need create new project, project id = {}", projectDto.getId());
            return create(projectDto, user);
        }

        // check crontab expression
        if (StringUtils.isNotEmpty(projectDto.getCrontabExpression())) {
            try {
                CronScheduleBuilder.cronSchedule(projectDto.getCrontabExpression());
            } catch (Exception e) {
                throw new BizException("Project.CronError");
            }
        }

        // check project name
        checkProjectName(projectDto.getName(), user, projectDto.getId());

        return save(projectDto, user);
    }

    /**
     * list all task of project, usring ProjectDto
     * @return 
     */
    private List<TaskDto> listAllTaskOfProject(ProjectDto projectDto) {
        return projectDto.getTasks().values().stream().collect(Collectors.toList());
    }

    /**
     * checkExistById
     * 检查项目是否存在
     * @param id
     * @param user
     * @return
     */
    public ProjectDto checkExistById(ObjectId id, UserDetail user) {
        ProjectDto projectDto = findById(id, user);
        if (projectDto == null) {
            throw new BizException("Project.NotFound");
        }
        return projectDto;
    }

    /**
     * 删除项目，删除Project及其关联的Task
     * 添加事务管理确保原子性操作
     * @param id
     * @param user
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public ProjectDto remove(ObjectId id, UserDetail user) {
        ProjectDto projectDto = checkExistById(id, user);
        
        try {
            // 先删除关联的所有任务
            List<TaskDto> tasks = listAllTaskOfProject(projectDto);
            for (TaskDto task : tasks) {
                taskService.remove(task.getId(), user);
            }
            
            // 再删除项目本身
            Update update = new Update().set(IS_DELETED, true);
            update(new Query(Criteria.where("_id").is(id)), update);
            
            return projectDto;
        } catch (Exception e) {
            log.error("Failed to remove project and its tasks, projectId: {}, error: {}", id, e.getMessage());
            throw new BizException("Project.RemoveFailed", e);
        }
    }

    @Override
    public void renew(ObjectId id, UserDetail user) {
        throw new UnsupportedOperationException("Project does not support renew operation");
    }

    @Override
    public void beforeSave(ProjectDto projectDto, UserDetail user) {
        // TODO: 添加项目保存前的检查逻辑
    }

    /**
     * 检查cron表达式是否正确
     * @param projectDto
     */
    private void checkCron(ProjectDto projectDto) {
        if (StringUtils.isNotEmpty(projectDto.getCrontabExpression())) {
            try {
                CronScheduleBuilder.cronSchedule(projectDto.getCrontabExpression());
            } catch (Exception e) {
                throw new BizException("Project.CronError");
            }
        }
    }

    /**
     * 检查任务顺序关系，按照依赖关系，将任务按照Dag方式进行组织
     * @param projectDto
     * @return TaskDag 任务依赖关系图
     */
    private TaskDag checkTaskOrder(ProjectDto projectDto) {
        TaskDag dag = new TaskDag();
        
        // 1. 获取所有任务
        List<TaskDto> tasks = listAllTaskOfProject(projectDto);
        Map<String, TaskDto> taskMap = tasks.stream()
                .collect(Collectors.toMap(task -> task.getName(), task -> task));
                
        // 2. 初始化DAG节点
        tasks.forEach(task -> {
            String taskName = task.getName();
            String taskId = task.getId().toHexString();
            TaskDag.TaskNode node = new TaskDag.TaskNode(taskName, taskId, task);
            dag.getNodes().put(taskName, node);
            dag.getDependencies().put(taskName, new ArrayList<>());
        });
        
        // 3. 构建依赖关系
        Map<String, String> taskDepends = projectDto.getTaskDepends();
        if (taskDepends != null) {
            for (Map.Entry<String, String> entry : taskDepends.entrySet()) {
                String taskName = entry.getKey();
                String dependInfo = entry.getValue();
                String[] dependInfoArray = dependInfo.split("\\.");
                
                // 检查依赖信息格式
                if (dependInfoArray.length != 3) {
                    throw new BizException("Project.TaskDependencyFormatError", 
                        "Task dependency format error, should be [taskName].[dependType].[dependEvent]");
                }
                
                String dependTaskName = dependInfoArray[0];
                String dependType = dependInfoArray[1];
                String dependEvent = dependInfoArray[2];
                
                // 检查依赖任务是否存在
                if (!taskMap.containsKey(taskName) || !taskMap.containsKey(dependTaskName)) {
                    throw new BizException("Project.TaskDependencyNotFound", 
                        String.format("Task %s or %s not found", taskName, dependTaskName));
                }
                
                // 添加依赖关系
                TaskDag.TaskDependency dependency = new TaskDag.TaskDependency(taskName, dependType, dependEvent);
                dag.getDependencies().get(dependTaskName).add(dependency);
                dag.getNodes().get(taskName).setInDegree(
                    dag.getNodes().get(taskName).getInDegree() + 1
                );
            }
        }
        
        // 4. 按层次构建可并行执行的任务组
        while (true) {
            Set<String> currentLayer = new HashSet<>();
            
            // 找出当前所有入度为0的节点
            dag.getNodes().forEach((taskName, node) -> {
                if (node.getInDegree() == 0) {
                    currentLayer.add(taskName);
                }
            });
            
            // 如果没有入度为0的节点，说明处理完成或存在环
            if (currentLayer.isEmpty()) {
                break;
            }
            
            // 将当前层添加到执行组
            dag.getParallelExecutionGroups().add(currentLayer);
            
            // 更新图，移除当前层的节点影响
            for (String taskName : currentLayer) {
                // 将节点的入度设置为-1，标记为已处理
                dag.getNodes().get(taskName).setInDegree(-1);
                
                // 更新依赖于该节点的其他节点的入度
                for (TaskDag.TaskDependency dependency : dag.getDependencies().get(taskName)) {
                    String nextTaskName = dependency.getTaskName();
                    TaskDag.TaskNode nextNode = dag.getNodes().get(nextTaskName);
                    if (nextNode.getInDegree() > 0) {  // 只更新未处理的节点
                        nextNode.setInDegree(nextNode.getInDegree() - 1);
                    }
                }
            }
        }
        
        // 5. 检查是否存在环
        int totalProcessedTasks = dag.getParallelExecutionGroups().stream()
                .mapToInt(Set::size)
                .sum();
        if (totalProcessedTasks != tasks.size()) {
            throw new BizException("Project.CyclicDependency", 
                "Cyclic dependency detected in task dependencies");
        }
        
        return dag;
    }

    /**
     * 启动Project
     * 1. 检查是否存在Project
     * 2. 检查cron表达式是否正确
     * 3. 检查任务顺序关系，构建DAG
     * 4. 根据DAG的执行顺序（先执行度为0的任务），创建任务并监听事件，当事件发生，调用回调函数
     * @param id
     * @param user
     */
    @Override
    public void start(ObjectId id, UserDetail user) {
        // 1. 检查是否存在Project
        ProjectDto projectDto = checkExistById(id, user);
        // 2. 检查cron表达式是否正确
        checkCron(projectDto);
        // 3. 检查任务顺序关系，构建DAG
        TaskDag taskDag = checkTaskOrder(projectDto);
        
        // 4. 首先启动入度为0的任务（没有依赖的任务）
        Set<String> initialTasks = taskDag.getParallelExecutionGroups().get(0);
        initialTasks.forEach(taskName -> {
            TaskDto taskDto = taskDag.getNodes().get(taskName).getTaskDto();
            createAndStartTask(taskDto, taskDag, user);
        });
    }

    /**
     * 创建并启动任务，同时注册任务阶段事件监听器
     */
    private void createAndStartTask(TaskDto taskDto, TaskDag dag, UserDetail user) {
        // 1. 创建任务
        TaskDto createdTask = taskService.create(taskDto, user);
        
        // 2. 注册任务阶段事件监听器
        registerTaskEventListener(createdTask, dag, user);
        
        // 3. 启动任务
        taskService.start(createdTask.getId(), user);
    }

    /**
     * 注册任务阶段事件监听器
     */
    private void registerTaskEventListener(TaskDto task, TaskDag dag, UserDetail user) {
        String taskName = task.getName();
        // 获取依赖于该任务的所有任务
        List<TaskDag.TaskDependency> dependencies = dag.getDependencies().get(taskName);
        
        if (dependencies != null && !dependencies.isEmpty()) {
            // 注册 initial_sync 阶段的事件监听器
            taskService.registerPhaseEventListener(task.getId(), "initial_sync", event -> {
                handleTaskEvent("initial_sync", event, taskName, dependencies, dag, user);
            });
            
            // 注册 cdc 阶段的事件监听器
            taskService.registerPhaseEventListener(task.getId(), "cdc", event -> {
                handleTaskEvent("cdc", event, taskName, dependencies, dag, user);
            });
        }
    }

    /**
     * 处理任务阶段事件
     */
    private void handleTaskEvent(String phase, String event, String taskName, 
            List<TaskDependency> dependencies, TaskDag dag, UserDetail user) {
        // 检查是否有任务依赖于当前事件
        dependencies.stream()
            .filter(dep -> dep.getDependType().equals(phase) && dep.getDependEvent().equals(event))
            .forEach(dep -> {
                TaskNode dependentNode = dag.getNodes().get(dep.getTaskName());
                // 减少入度
                dependentNode.setInDegree(dependentNode.getInDegree() - 1);
                
                // 如果入度变为0，说明所有依赖都满足了，可以启动该任务
                if (dependentNode.getInDegree() == 0) {
                    createAndStartTask(dependentNode.getTaskDto(), dag, user);
                }
            });
    }

    @Override
    public void stop(ObjectId id, UserDetail user) {
        throw new UnsupportedOperationException("Project does not support stop operation");
    }

}

