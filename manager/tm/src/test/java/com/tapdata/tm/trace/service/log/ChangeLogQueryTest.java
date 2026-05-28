package com.tapdata.tm.trace.service.log;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.trace.ChangeLogCriteria;
import com.tapdata.tm.commons.trace.ChangeLogData;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.trace.dto.ChangeLog;
import com.tapdata.tm.trace.param.ChangeLogParam;
import com.tapdata.tm.trace.service.data.TraceDataQueryRpcAdapter;
import com.tapdata.tm.utils.MessageUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChangeLogQueryTest {
    private DataSourceRepository dataSourceRepository;
    private ShareCdcTableMappingRepository shareCdcTableMappingRepository;
    private TraceDataQueryRpcAdapter traceDataQueryRpcAdapter;
    private ExternalStorageService externalStorageService;
    private ChangeLogQuery changeLogQuery;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        dataSourceRepository = mock(DataSourceRepository.class);
        shareCdcTableMappingRepository = mock(ShareCdcTableMappingRepository.class);
        traceDataQueryRpcAdapter = mock(TraceDataQueryRpcAdapter.class);
        externalStorageService = mock(ExternalStorageService.class);
        user = mock(UserDetail.class);

        changeLogQuery = new ChangeLogQuery();
        changeLogQuery.dataSourceRepository = dataSourceRepository;
        changeLogQuery.shareCdcTableMappingRepository = shareCdcTableMappingRepository;
        changeLogQuery.traceDataQueryRpcAdapter = traceDataQueryRpcAdapter;
        changeLogQuery.externalStorageService = externalStorageService;
    }

    @Test
    void query_shouldValidateRequiredParams() {
        ChangeLogParam p1 = baseParam();
        p1.setConnectionId(" ");
        BizException e1 = assertThrows(BizException.class, () -> changeLogQuery.query(p1, user));
        assertEquals("schema.reload.connectionId", e1.getErrorCode());

        ChangeLogParam p2 = baseParam();
        p2.setTable(" ");
        BizException e2 = assertThrows(BizException.class, () -> changeLogQuery.query(p2, user));
        assertEquals("schema.reload.tableName", e2.getErrorCode());

        ChangeLogParam p3 = baseParam();
        p3.setStartTime(null);
        BizException e3 = assertThrows(BizException.class, () -> changeLogQuery.query(p3, user));
        assertEquals("data.trace.log.sTime", e3.getErrorCode());

        ChangeLogParam p4 = baseParam();
        p4.setEndTime(null);
        BizException e4 = assertThrows(BizException.class, () -> changeLogQuery.query(p4, user));
        assertEquals("data.trace.log.eTime", e4.getErrorCode());

        ChangeLogParam p5 = baseParam();
        p5.setStartTime(0L);
        p5.setEndTime(8L * 24 * 60 * 60 * 1000);
        BizException e5 = assertThrows(BizException.class, () -> changeLogQuery.query(p5, user));
        assertEquals("data.trace.log.time.too.large", e5.getErrorCode());
    }

    @Test
    void findShareCDCExternalStorageId_shouldThrowWhenInvalidConnectionId() {
        BizException e = assertThrows(BizException.class, () -> changeLogQuery.findShareCDCExternalStorageId("invalid", user));
        assertEquals("schema.reload.connectionId.invalid", e.getErrorCode());
    }

    @Test
    void findShareCDCExternalStorageId_shouldReturnNullWhenNotFound() {
        String connectionId = "507f1f77bcf86cd799439011";
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.empty());
        assertNull(changeLogQuery.findShareCDCExternalStorageId(connectionId, user));
    }

    @Test
    void findShareCDCExternalStorageId_shouldReturnIdFromEntity() {
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceEntity e = new DataSourceEntity();
        e.setShareCDCExternalStorageId("507f1f77bcf86cd799439012");
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(e));
        assertEquals("507f1f77bcf86cd799439012", changeLogQuery.findShareCDCExternalStorageId(connectionId, user));
    }

    @Test
    void findTableRingBufferId_shouldReturnNullWhenNotFound_orValueWhenFound() {
        when(shareCdcTableMappingRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.empty());
        assertNull(changeLogQuery.findTableRingBufferId("c", "t", user));

        ShareCdcTableMappingEntity entity = new ShareCdcTableMappingEntity();
        entity.setExternalStorageTableName("rb");
        when(shareCdcTableMappingRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(entity));
        assertEquals("rb", changeLogQuery.findTableRingBufferId("c", "t", user));
    }

    @Test
    void query_shouldReturnEmpty_whenShareCdcExternalStorageIdBlankOrStorageNull() {
        ChangeLogParam p = baseParam();

        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(new DataSourceEntity()));
        ChangeLog r1 = changeLogQuery.query(p, user);
        assertNotNull(r1);
        assertEquals(List.of(), r1.getLogs());
        verify(externalStorageService, never()).findById(any(ObjectId.class));

        DataSourceEntity entity = new DataSourceEntity();
        entity.setShareCDCExternalStorageId("507f1f77bcf86cd799439012");
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(entity));
        when(externalStorageService.findById(any(ObjectId.class))).thenReturn(null);
        ChangeLog r2 = changeLogQuery.query(p, user);
        assertEquals(List.of(), r2.getLogs());
    }

    @Test
    void query_shouldReturnMsg_whenUnsupportedExternalStorageType() {
        ChangeLogParam p = baseParam();

        DataSourceEntity entity = new DataSourceEntity();
        entity.setShareCDCExternalStorageId("507f1f77bcf86cd799439012");
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(entity));

        ExternalStorageDto storageDto = new ExternalStorageDto();
        storageDto.setType("Anything");
        when(externalStorageService.findById(any(ObjectId.class))).thenReturn(storageDto);

        try (MockedStatic<MessageUtil> mockedMessage = org.mockito.Mockito.mockStatic(MessageUtil.class)) {
            mockedMessage.when(() -> MessageUtil.getMessage(eq("data.trace.log.supported"), eq("Anything"))).thenReturn("m");
            ChangeLog r = changeLogQuery.query(p, user);
            assertEquals("m", r.getMsg());
            assertEquals(List.of(), r.getLogs());
        }
    }

    @Test
    void query_shouldReturnEmpty_whenRingBufferBlank() {
        ChangeLogParam p = baseParam();

        DataSourceEntity entity = new DataSourceEntity();
        entity.setShareCDCExternalStorageId("507f1f77bcf86cd799439012");
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(entity));

        ExternalStorageDto storageDto = new ExternalStorageDto();
        storageDto.setType("MongoDB");
        when(externalStorageService.findById(any(ObjectId.class))).thenReturn(storageDto);

        when(shareCdcTableMappingRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(new ShareCdcTableMappingEntity()));

        try (MockedStatic<ExternalStorageType> mocked = org.mockito.Mockito.mockStatic(ExternalStorageType.class)) {
            mocked.when(() -> ExternalStorageType.supported(eq("MongoDB"))).thenReturn(true);
            ChangeLog r = changeLogQuery.query(p, user);
            assertEquals(List.of(), r.getLogs());
            verify(traceDataQueryRpcAdapter, never()).queryChangeLog(any());
        }
    }

    @Test
    void query_shouldBuildCriteriaAndReturnLogs_whenSupportedAndRingBufferPresent() {
        ChangeLogParam p = baseParam();
        p.setQueryConditions(List.of(Map.of("k", "v")));
        p.setLimit(5);
        p.setLastKey(9L);

        DataSourceEntity entity = new DataSourceEntity();
        entity.setShareCDCExternalStorageId("507f1f77bcf86cd799439012");
        when(dataSourceRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(entity));

        ExternalStorageDto storageDto = new ExternalStorageDto();
        storageDto.setType("MongoDB");
        when(externalStorageService.findById(any(ObjectId.class))).thenReturn(storageDto);

        ShareCdcTableMappingEntity mappingEntity = new ShareCdcTableMappingEntity();
        mappingEntity.setExternalStorageTableName("rb1");
        when(shareCdcTableMappingRepository.findOne(any(Query.class), eq(user))).thenReturn(Optional.of(mappingEntity));

        ChangeLogData data = new ChangeLogData();
        data.setLogs(List.of(Map.of("a", 1)));
        data.setLastKey(10L);
        when(traceDataQueryRpcAdapter.queryChangeLog(any())).thenReturn(data);

        try (MockedStatic<ExternalStorageType> mocked = org.mockito.Mockito.mockStatic(ExternalStorageType.class)) {
            mocked.when(() -> ExternalStorageType.supported(eq("MongoDB"))).thenReturn(true);

            ChangeLog r = changeLogQuery.query(p, user);
            assertEquals(List.of(Map.of("a", 1)), r.getLogs());
            assertEquals(10L, r.getLastKey());

            ArgumentCaptor<ChangeLogCriteria> captor = ArgumentCaptor.forClass(ChangeLogCriteria.class);
            verify(traceDataQueryRpcAdapter).queryChangeLog(captor.capture());
            ChangeLogCriteria c = captor.getValue();
            assertEquals("rb1", c.getRingBuffer());
            assertEquals(p.getConnectionId(), c.getConnectionId());
            assertEquals(p.getTable(), c.getTableName());
            assertEquals(p.getQueryConditions(), c.getFilters());
            assertEquals(entity.getShareCDCExternalStorageId(), c.getExternalStorageId());
            assertEquals(p.getStartTime().longValue(), c.getStartTime());
            assertEquals(p.getEndTime().longValue(), c.getEndTime());
            assertEquals(p.getLimit(), c.getLimit());
            assertSame(p.getLastKey(), c.getKey());
        }
    }

    private static ChangeLogParam baseParam() {
        ChangeLogParam p = new ChangeLogParam();
        p.setConnectionId("507f1f77bcf86cd799439011");
        p.setTable("t");
        p.setStartTime(1_000L);
        p.setEndTime(2_000L);
        return p;
    }
}
