package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
import com.tapdata.tm.ds.exception.ConnectionAgentConflictException;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DataSourceAgentSwitchTest {
    @Test
    void singleConnectionUpdateCannotBypassConflictValidation() {
        DataSourceRepository repository = mock(DataSourceRepository.class);
        DataSourceServiceImpl service = spy(new DataSourceServiceImpl(repository));
        ConnectionAgentConflictService checker = mock(ConnectionAgentConflictService.class);
        WorkerService workers = mock(WorkerService.class);
        ReflectionTestUtils.setField(service, "connectionAgentConflictService", checker);
        ReflectionTestUtils.setField(service, "workerService", workers);
        when(workers.findAvailableAgentByAccessNode(any(), anyList())).thenReturn(List.of(new Worker()));
        UserDetail user = mock(UserDetail.class);
        ObjectId id = new ObjectId();
        DataSourceConnectionDto stored = new DataSourceConnectionDto();
        stored.setId(id); stored.setName("source"); stored.setUserId(new ObjectId().toHexString());
        stored.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name()); stored.setAccessNodeProcessId("agent-a");
        DataSourceConnectionDto submitted = new DataSourceConnectionDto();
        submitted.setId(id); submitted.setDatabase_type("dummy");
        submitted.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        submitted.setAccessNodeProcessId("agent-b"); submitted.setAccessNodeProcessIdList(List.of("agent-b"));
        doReturn("source").when(service).updateCheck(user, submitted);
        doReturn(stored).when(service).findById(id);
        ConnectionAgentConflictReport report = new ConnectionAgentConflictReport(); report.setHasConflicts(true);
        doThrow(new ConnectionAgentConflictException(report)).when(checker).assertNoConflicts(anyMap(), eq(user));
        when(repository.count(any(Query.class), eq(user))).thenReturn(1L);
        try (MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class)) {
            permissions.when(() -> DataPermissionHelper.check(any(), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(call -> ((java.util.function.Supplier<?>) call.getArgument(5)).get());
            when(repository.count(any(Query.class), eq(user))).thenReturn(0L);
            com.tapdata.tm.base.exception.BizException denied = assertThrows(com.tapdata.tm.base.exception.BizException.class,
                    () -> service.update(user, submitted, true));
            assertEquals("Datasource.NotFound", denied.getErrorCode());
            verifyNoInteractions(checker);
            when(repository.count(any(Query.class), eq(user))).thenReturn(1L);
            assertThrows(ConnectionAgentConflictException.class, () -> service.update(user, submitted, true));
        }
        ArgumentCaptor<Map<String, DataSourceConnectionDto>> changes = ArgumentCaptor.forClass(Map.class);
        verify(checker).assertNoConflicts(changes.capture(), eq(user));
        DataSourceConnectionDto proposed = changes.getValue().get(id.toHexString());
        assertEquals("agent-b", proposed.getAccessNodeProcessId());
        assertEquals(stored.getUserId(), proposed.getUserId());
        assertEquals("agent-a", stored.getAccessNodeProcessId());
        verify(repository, never()).buildUpdateSet(any(), any());
    }
    @Test
    void ordinaryConnectionEditDoesNotCheckOrClearTheExistingGroupPriority() {
        DataSourceRepository repository = mock(DataSourceRepository.class);
        DataSourceServiceImpl service = spy(new DataSourceServiceImpl(repository));
        ConnectionAgentConflictService checker = mock(ConnectionAgentConflictService.class);
        ReflectionTestUtils.setField(service, "connectionAgentConflictService", checker);
        UserDetail user = mock(UserDetail.class);
        ObjectId id = new ObjectId();
        DataSourceConnectionDto stored = new DataSourceConnectionDto();
        stored.setId(id); stored.setName("source");
        stored.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        stored.setAccessNodeProcessId("existing-group"); stored.setPriorityProcessId("priority-agent");
        DataSourceConnectionDto patch = new DataSourceConnectionDto();
        patch.setId(id); patch.setName("renamed"); patch.setDatabase_type("dummy");
        doReturn("source").when(service).updateCheck(user, patch);
        doReturn(stored).when(service).findById(id);
        RuntimeException reachedPersistence = new RuntimeException("persistence checkpoint");
        when(repository.buildUpdateSet(any(), eq(user))).thenThrow(reachedPersistence);
        assertSame(reachedPersistence, assertThrows(RuntimeException.class, () -> service.update(user, patch, true)));
        verifyNoInteractions(checker);
        assertEquals("priority-agent", stored.getPriorityProcessId());
    }
}
