package com.tapdata.tm.modules.service;

import com.mongodb.client.MongoClients;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Timeout(30)
@EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
class PublishAtomicMongoIT {
    @Test
    void concurrent_publications_cannot_store_another_operations_configuration() throws Exception {
        try (var client = MongoClients.create(System.getProperty("audit.it.mongoUri"))) {
            var mongo = new MongoTemplate(client, "audit_publish_" + UUID.randomUUID().toString().replace("-", ""));
            var executor = Executors.newFixedThreadPool(2);
            Object previousHelper = ReflectionTestUtils.getField(DataPermissionHelper.class, "iDataPermissionHelper");
            new DataPermissionHelper(mock(IDataPermissionHelper.class));
            try {
                var repository = new ModulesRepository(mongo);
                var service = new ModulesService(repository);
                ReflectionTestUtils.setField(service, "mongoOperations", mongo);
                var user = new UserDetail("user", "tenant", "admin", "password", List.of(new SimpleGrantedAuthority("ADMIN")));
                for (int i = 0; i < 20; i++) {
                    var id = new ObjectId();
                    var original = new ModulesEntity();
                    original.setId(id);
                    original.setName("initial");
                    original.setCustomId("tenant");
                    original.setUserId("user");
                    original.setStatus(ModuleStatusEnum.PENDING.getValue());
                    mongo.insert(original);
                    var barrier = new CyclicBarrier(2);
                    var a = executor.submit(() -> publish(service, user, id, "name-a", "operation-a", barrier));
                    var b = executor.submit(() -> publish(service, user, id, "name-b", "operation-b", barrier));
                    assertThat((a.get() ? 1 : 0) + (b.get() ? 1 : 0)).isEqualTo(1);
                    var actual = mongo.findById(id, ModulesEntity.class);
                    assertThat(actual).isNotNull();
                    assertThat(actual.getStatus()).isEqualTo(ModuleStatusEnum.ACTIVE.getValue());
                    assertThat(actual.getName() + ":" + actual.getAuditPublishOperationId())
                            .isIn("name-a:operation-a", "name-b:operation-b");
                }
            } finally {
                ReflectionTestUtils.setField(DataPermissionHelper.class, "iDataPermissionHelper", previousHelper);
                executor.shutdownNow();
                mongo.getDb().drop();
            }
        }
    }

    private boolean publish(ModulesService service, UserDetail user, ObjectId id, String name,
                            String operation, CyclicBarrier barrier) throws Exception {
        var dto = new ModulesDto();
        dto.setId(id);
        dto.setName(name);
        dto.setStatus(ModuleStatusEnum.ACTIVE.getValue());
        barrier.await();
        try {
            ReflectionTestUtils.invokeMethod(service, "savePublishedConfiguration", dto, user, operation, true);
            return true;
        } catch (com.tapdata.tm.base.exception.BizException expected) {
            return false;
        }
    }
}
