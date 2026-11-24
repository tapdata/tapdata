package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.*;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.inspect.bean.*;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
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
        ObjectId connectionId = new ObjectId(request.getConnectionId());
        DataSourceConnectionDto connection = DataPermissionHelper.checkOfQuery(user,
                DataPermissionDataTypeEnums.Connections, DataPermissionActionEnums.View,
                dataSourceService.dataPermissionFindById(connectionId, new Field()),
                dto -> DataPermissionMenuEnums.Connections,
                () -> dataSourceService.getById(connectionId, new Field(), true, user),
                () -> { throw new BizException("insufficient.permissions"); });
        if (connection == null) throw new IllegalArgumentException("Connection is unavailable or inaccessible");
        if (connection.getCapabilities() == null || connection.getCapabilities().stream().noneMatch(c -> "execute_command_function".equals(c.getId())))
            throw new IllegalArgumentException("Connection does not support executeCommand queries");
    }
    /** Called by the enterprise InspectServiceImpl save hook and update validation before persistence. */
    public void validate(InspectDto inspect, UserDetail user) {
        if (!"custom_query".equals(inspect.getInspectMethod())) return;
        // An absent task list on an existing task is an unchanged PATCH field, not an empty replacement.
        if (inspect.getTasks() == null && inspect.getId() != null) return;
        if (inspect.getTasks() == null || inspect.getTasks().isEmpty()) throw new IllegalArgumentException("Custom query tasks are required");
        for (Task task : inspect.getTasks()) {
            CustomQueryConfig config = task.getCustomQueryConfig();
            if (config == null || task.getSource() == null || task.getTarget() == null) throw new IllegalArgumentException("Test both queries and confirm field mappings for this custom query task");
            // Reuse the existing task-level error retention control.
            if (inspect.getLimit() != null && inspect.getLimit().getKeep() != null) config.setDetailLimit(inspect.getLimit().getKeep());
            config.validate(task.getSource().getQuerySnapshot(), task.getTarget().getQuerySnapshot());
            validateSide(task.getSource(), config, user); validateSide(task.getTarget(), config, user);
        }
    }
    private void validateSide(Source side, CustomQueryConfig config, UserDetail user) {
        CustomQueryRequest request = new CustomQueryRequest();
        request.setConnectionId(side.getConnectionId()); request.setQuery(side.getCustomQuery()); request.setKind(side.getQueryKind());
        request.setCollection(side.getTable()); request.setTimezone(CustomQueryRequest.effectiveTimezone(side.getQueryTimezone(), side.getQuerySnapshot())); request.setTimeoutSeconds(config.getTimeoutSeconds()); request.setIgnoreCase(config.isIgnoreCase());
        authorize(request, user);
        if (side.getQuerySnapshot() == null || !request.fingerprint().equals(side.getQuerySnapshot().getFingerprint()))
            throw new IllegalArgumentException("Query or connection changed; test again before saving");
        // Persist column snapshots only. Preview rows are transient and must not inflate task documents.
        side.getQuerySnapshot().setRows(new java.util.ArrayList<>());
    }
}
