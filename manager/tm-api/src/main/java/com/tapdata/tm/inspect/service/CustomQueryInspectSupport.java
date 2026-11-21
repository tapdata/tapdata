package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.*;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.inspect.bean.*;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CustomQueryInspectSupport {
    @Autowired private DataSourceService dataSourceService;

    public void authorize(CustomQueryRequest request, UserDetail user) {
        request.validate();
        DataSourceConnectionDto connection = DataPermissionMenuEnums.Connections.checkAndSetFilter(user,
                DataPermissionActionEnums.View, () -> dataSourceService.getById(new ObjectId(request.getConnectionId()), new Field(), true, user));
        if (connection == null) throw new IllegalArgumentException("Connection is unavailable or inaccessible");
        if (connection.getCapabilities() == null || connection.getCapabilities().stream().noneMatch(c -> "execute_command_function".equals(c.getId())))
            throw new IllegalArgumentException("Connection does not support executeCommand queries");
    }
    public void validate(InspectDto inspect, UserDetail user) {
        if (!"custom_query".equals(inspect.getInspectMethod())) return;
        if (inspect.getTasks() == null || inspect.getTasks().isEmpty()) throw new IllegalArgumentException("Custom query tasks are required");
        for (Task task : inspect.getTasks()) {
            CustomQueryConfig config = task.getCustomQueryConfig();
            if (config == null || task.getSource() == null || task.getTarget() == null) throw new IllegalArgumentException("Test both queries and confirm field mappings for this custom query task");
            config.validate(task.getSource().getQuerySnapshot(), task.getTarget().getQuerySnapshot());
            // Reuse the existing task-level error retention control.
            if (inspect.getLimit() != null && inspect.getLimit().getKeep() != null) config.setDetailLimit(inspect.getLimit().getKeep());
            config.validate(task.getSource().getQuerySnapshot(), task.getTarget().getQuerySnapshot());
            validateSide(task.getSource(), config, user); validateSide(task.getTarget(), config, user);
        }
    }
    private void validateSide(Source side, CustomQueryConfig config, UserDetail user) {
        CustomQueryRequest request = new CustomQueryRequest();
        request.setConnectionId(side.getConnectionId()); request.setQuery(side.getCustomQuery()); request.setKind(side.getQueryKind());
        request.setCollection(side.getTable()); request.setTimezone(side.getQueryTimezone()); request.setTimeoutSeconds(config.getTimeoutSeconds());
        authorize(request, user);
        if (side.getQuerySnapshot() == null || !request.fingerprint().equals(side.getQuerySnapshot().getFingerprint()))
            throw new IllegalArgumentException("Query or connection changed; test again before saving");
        // Persist column snapshots only. Preview rows are transient and must not inflate task documents.
        side.getQuerySnapshot().setRows(new java.util.ArrayList<>());
    }
}
