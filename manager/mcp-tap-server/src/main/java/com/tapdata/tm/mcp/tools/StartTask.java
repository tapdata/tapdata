package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
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

import static com.tapdata.tm.mcp.Utils.getStringValue;
import static com.tapdata.tm.mcp.Utils.readJsonSchema;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Slf4j
@Component
public class StartTask extends Tool {

    private final TaskService taskService;

    public StartTask(SessionAttribute sessionAttribute,
                     UserService userService,
                     TaskService taskService) {
        super("startTask",
                "Start one or more TapData tasks. If a task is in edit status, confirm it first so it becomes wait_start before starting.",
                readJsonSchema("StartTask.json"), sessionAttribute, userService);
        this.taskService = taskService;
    }

    @Override
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {
        UserDetail userDetail = getUserDetail(exchange);
        List<String> taskIds = resolveTaskIds(params);
        if (taskIds.isEmpty()) {
            throw new RuntimeException("Parameter taskId or taskIds is required.");
        }

        List<ObjectId> objectIds = taskIds.stream().map(id -> toObjectId(id)).collect(Collectors.toList());
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
        response.put("taskIds", taskIds);
        response.put("confirmedTaskIds", confirmedTaskIds);
        response.put("tasks", taskStatuses);
        response.put("startResults", startResults);
        response.put("message", "Task start request submitted.");

        return makeCallToolResult(response);
    }

    private ServletRequestAttributes getCurrentRequestAttributes() {
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (requestAttributes instanceof ServletRequestAttributes servletRequestAttributes) {
            return servletRequestAttributes;
        }
        return null;
    }

    private List<String> resolveTaskIds(Map<String, Object> params) {
        Set<String> ids = new LinkedHashSet<>();
        String taskId = getStringValue(params, "taskId");
        if (StringUtils.isNotBlank(taskId)) {
            ids.add(taskId);
        }

        Object taskIdsParam = params == null ? null : params.get("taskIds");
        if (taskIdsParam instanceof List<?> taskIds) {
            for (Object id : taskIds) {
                if (id != null && StringUtils.isNotBlank(id.toString())) {
                    ids.add(id.toString());
                }
            }
        }

        return new ArrayList<>(ids);
    }
}
