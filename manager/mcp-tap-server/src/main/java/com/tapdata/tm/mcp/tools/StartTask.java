package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Slf4j
@Component
public class StartTask {

    private final McpToolSupport toolSupport;
    private final TaskService taskService;

    public StartTask(McpToolSupport toolSupport,
                     TaskService taskService) {
        this.toolSupport = toolSupport;
        this.taskService = taskService;
    }

    @McpTool(name = "startTask", description = "Start one or more TapData tasks. Tasks in edit status are confirmed before starting.")
    public Map<String, Object> startTask(
            McpSyncRequestContext context,
            @McpToolParam(required = false, description = "Single TapData task id to start.") String taskId,
            @McpToolParam(required = false, description = "Multiple TapData task ids to start.") List<String> taskIds) {
        UserDetail userDetail = toolSupport.getUserDetail(context);
        List<String> resolvedTaskIds = resolveTaskIds(taskId, taskIds);
        if (resolvedTaskIds.isEmpty()) {
            throw new RuntimeException("Parameter taskId or taskIds is required.");
        }

        List<ObjectId> objectIds = resolvedTaskIds.stream().map(id -> toObjectId(id)).collect(Collectors.toList());
        List<Map<String, Object>> taskStatuses = new ArrayList<>();
        List<String> confirmedTaskIds = new ArrayList<>();

        for (ObjectId objectId : objectIds) {
            TaskDto taskDto = taskService.checkExistById(objectId, userDetail);
            if (taskDto == null) {
                throw new RuntimeException("Task not found: " + objectId.toHexString());
            }

            String statusBeforeStart = taskDto.getStatus();
            boolean confirmed = false;
            if (TaskDto.STATUS_EDIT.equals(statusBeforeStart)) {
                taskDto = taskService.confirmById(taskDto, userDetail, true);
                taskDto = taskService.checkExistById(objectId, userDetail);
                confirmed = true;
                confirmedTaskIds.add(objectId.toHexString());
            }

            Map<String, Object> status = new LinkedHashMap<>();
            status.put("id", objectId.toHexString());
            status.put("name", taskDto.getName());
            status.put("statusBeforeStart", statusBeforeStart);
            status.put("confirmed", confirmed);
            status.put("statusAfterConfirm", taskDto.getStatus());
            taskStatuses.add(status);
        }

        taskService.clearAgentAffinityForManualStart(objectIds, userDetail);
        ServletRequestAttributes requestAttributes = getCurrentRequestAttributes();
        HttpServletRequest request = requestAttributes == null ? null : requestAttributes.getRequest();
        HttpServletResponse httpResponse = requestAttributes == null ? null : requestAttributes.getResponse();
        List<MutiResponseMessage> startResults = taskService.batchStart(objectIds, userDetail, request, httpResponse);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("taskIds", resolvedTaskIds);
        response.put("confirmedTaskIds", confirmedTaskIds);
        response.put("tasks", taskStatuses);
        response.put("startResults", startResults);
        response.put("message", "Task start request submitted.");

        return response;
    }

    private ServletRequestAttributes getCurrentRequestAttributes() {
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (requestAttributes instanceof ServletRequestAttributes servletRequestAttributes) {
            return servletRequestAttributes;
        }
        return null;
    }

    private List<String> resolveTaskIds(String taskId, List<String> taskIds) {
        Set<String> ids = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(taskId)) {
            ids.add(taskId);
        }

        if (taskIds != null) {
            for (String id : taskIds) {
                if (StringUtils.isNotBlank(id)) {
                    ids.add(id);
                }
            }
        }

        return new ArrayList<>(ids);
    }
}
