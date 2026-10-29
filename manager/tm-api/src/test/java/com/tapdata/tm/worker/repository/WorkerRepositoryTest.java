package com.tapdata.tm.worker.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.worker.entity.Worker;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.test.util.ReflectionTestUtils;
import com.mongodb.client.result.UpdateResult;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class WorkerRepositoryTest {

    @Test
    void retriesUserDetailUpsertAfterDuplicateKey() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        UserDetail userDetail = mock(UserDetail.class);
        when(userDetail.getUserId()).thenReturn("user");
        when(userDetail.getCustomerId()).thenReturn("customer");
        when(userDetail.getUsername()).thenReturn("tester");
        when(userDetail.getAuthorities()).thenReturn(Collections.emptyList());
        when(userDetail.isRoot()).thenReturn(true);
        new DataPermissionHelper(mock(IDataPermissionHelper.class));

        UpdateResult success = UpdateResult.acknowledged(1, 1L, null);
        when(mongoTemplate.upsert(any(Query.class), any(Update.class), eq(Worker.class)))
                .thenThrow(new DuplicateKeyException("worker key raced"))
                .thenReturn(success);

        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", null);
        try (MockedConstruction<MongoRepositoryFactory> ignored = mockConstruction(
                MongoRepositoryFactory.class,
                (factory, context) -> {
                    MongoEntityInformation<Worker, Object> information = mock(MappingMongoEntityInformation.class);
                    when(factory.getEntityInformation(Worker.class)).thenReturn(information);
                })) {
            WorkerRepository repository = new WorkerRepository(mongoTemplate);
            Worker worker = new Worker();
            worker.setWorkerType("api-server");
            worker.setProcessId("api-server-1");

            assertDoesNotThrow(() -> repository.upsert(new Query(), worker, userDetail));

            verify(mongoTemplate, times(2)).upsert(any(Query.class), any(Update.class), eq(Worker.class));
        }
    }
}
