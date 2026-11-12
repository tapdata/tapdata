package com.tapdata.tm.worker.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.entity.WorkerExpire;
import com.tapdata.tm.worker.entity.field.WorkerType;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.WorkerOrServerStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerServiceImplTest {
    WorkerServiceImpl workerService;
    MongoTemplate mongoTemplate;
    @BeforeEach
    void init() {
        mongoTemplate = mock(MongoTemplate.class);
        workerService = mock(WorkerServiceImpl.class);
        ReflectionTestUtils.setField(workerService, "mongoTemplate", mongoTemplate);
    }

    @Test
    void filteredAgentLookupRetainsUserPermissionQuery() {
        WorkerRepository repository = mock(WorkerRepository.class);
        WorkerServiceImpl service = new WorkerServiceImpl(repository);
        SettingsService settings = mock(SettingsService.class);
        ReflectionTestUtils.setField(service, "settingsService", settings);
        UserDetail user = mock(UserDetail.class);

        service.findAllAgent(user, List.of("selected-process"));

        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(repository).findAll(query.capture(), same(user));
        Map<?, ?> filter = query.getValue().getQueryObject();
        assertEquals("connector", filter.get("worker_type"));
        assertEquals(true, ((Map<?, ?>) filter.get("isDeleted")).get("$ne"));
        assertEquals(List.of("selected-process"), ((Map<?, ?>) filter.get("process_id")).get("$in"));
    }

    @Test
    void filteredAgentLookupRetainsCloudSharedWorkerScope() {
        WorkerRepository repository = mock(WorkerRepository.class);
        WorkerServiceImpl service = new WorkerServiceImpl(repository);
        SettingsService settings = mock(SettingsService.class);
        MongoTemplate mongo = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(service, "settingsService", settings);
        ReflectionTestUtils.setField(service, "mongoTemplate", mongo);
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn("current-user");
        when(settings.isCloud()).thenReturn(true);
        WorkerExpire shared = new WorkerExpire();
        shared.setShareUser("shared-owner");
        shared.setExpireTime(new Date(System.currentTimeMillis() + 60_000L));
        when(mongo.findOne(any(Query.class), org.mockito.ArgumentMatchers.eq(WorkerExpire.class))).thenReturn(shared);

        service.findAllAgent(user, List.of("selected-process"));

        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(repository).findAll(query.capture());
        Map<?, ?> filter = query.getValue().getQueryObject();
        assertEquals(List.of("selected-process"), ((Map<?, ?>) filter.get("process_id")).get("$in"));
        String sharedFilter = filter.get("$or").toString();
        Assertions.assertTrue(sharedFilter.contains("current-user"));
        Assertions.assertTrue(sharedFilter.contains("shared-owner"));
    }

    @Nested
    class updateWorkerStatusTest {

        @Test
        void testNoral() {
            WorkerOrServerStatus status = new WorkerOrServerStatus();
            status.setStatus("ok");
            status.setWorkerStatus(new HashMap<>());
            status.getWorkerStatus().put("id", "ok");
            status.setCpuMemStatus(new HashMap<>());
            status.getCpuMemStatus().put("id", new MetricInfo());
            status.setWorkerBaseInfo(new HashMap<>());
            status.getWorkerBaseInfo().put("id", new ApiServerWorkerInfo());
            status.getWorkerBaseInfo().get("id").setName("name");
            status.getWorkerBaseInfo().get("id").setOid("oid");
            status.getWorkerBaseInfo().get("id").setId(1);
            status.getWorkerBaseInfo().get("id").setWorkerStartTime(1L);
            status.getWorkerBaseInfo().get("id").setSort(1);
            status.setProcessCpuMemStatus(new MetricInfo());
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUsername()).thenReturn("username");
            when(workerService.update(any(Query.class), any(Update.class))).thenReturn(null);
            doCallRealMethod().when(workerService).updateWorkerStatus(status, userDetail);
            Assertions.assertDoesNotThrow(() -> workerService.updateWorkerStatus(status, userDetail));
        }
    }

    @Nested
    class healthTest {
        @Test
        void healthUpsertUsesActiveDeletedFlag() {
            WorkerRepository repository = mock(WorkerRepository.class);
            WorkerServiceImpl service = new WorkerServiceImpl(repository);
            SettingsService settingsService = mock(SettingsService.class);
            Settings buildProfile = new Settings();
            buildProfile.setValue("CLOUD");
            ReflectionTestUtils.setField(service, "settingsService", settingsService);
            when(settingsService.getByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE))
                    .thenReturn(buildProfile);
            when(repository.upsert(any(Query.class), any(Worker.class), any(UserDetail.class))).thenReturn(1L);
            when(repository.findOne(any(Query.class))).thenReturn(Optional.empty());

            WorkerDto worker = new WorkerDto();
            worker.setProcessId("api-server-1");
            worker.setWorkerType(WorkerType.API_SERVER.getType());
            worker.setVersion("test-version");
            UserDetail userDetail = mock(UserDetail.class);

            service.health(worker, userDetail);

            ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
            verify(repository).upsert(queryCaptor.capture(), any(Worker.class), same(userDetail));
            assertEquals(true, ((Map)queryCaptor.getValue().getQueryObject().get("deleted")).get("$ne"));
        }
    }
}
