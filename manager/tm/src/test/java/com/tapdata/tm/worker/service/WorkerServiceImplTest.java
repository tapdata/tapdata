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

import java.util.HashMap;
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
