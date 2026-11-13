package com.tapdata.tm.application;

import com.mongodb.client.MongoClient;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.application.controller.ApplicationController;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * TAP-13258: a newly created (non-admin) user creates an API Client through
 * ApplicationController.save → ApplicationService → ApplicationRepository.
 */
class ApplicationNonAdminCreateIT {
    private Object previousFactory;

    @BeforeEach
    void resetRepositoryFactory() {
        previousFactory = ReflectionTestUtils.getField(BaseRepository.class, "repositoryFactory");
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", null);
    }

    @AfterEach
    void restoreRepositoryFactory() {
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", previousFactory);
    }

    @Test
    @DisplayName("非管理员创建 API Client：回填 clientId 的更新只命中新建记录")
    void should_create_api_client_as_non_admin_without_touching_other_clients() {
        ObjectId createdId = new ObjectId();

        Document mapped = saveAsNewUserAndCaptureUpdateFilter(createdId);

        assertThat(mapped.get("_id")).isEqualTo(createdId);
        assertThat(mapped).containsEntry("customId", "tenant-a");
        assertThat(mapped.getList("$or", Document.class)).containsExactly(
                new Document("user_id", "new-user-id"),
                new Document("user_id", new Document("$exists", false)));
    }

    @Test
    @DisplayName("非管理员创建 API Client：更新条件不会命中无归属记录或该用户已有的其它 API Client（code 66 场景）")
    void should_not_match_other_clients_that_would_raise_immutable_id_error() {
        ObjectId createdId = new ObjectId();
        Document created = new Document("_id", createdId).append("customId", "tenant-a").append("user_id", "new-user-id");
        Document legacyWithoutOwner = new Document("_id", new ObjectId()).append("customId", "tenant-a");
        Document ownedEarlier = new Document("_id", new ObjectId()).append("customId", "tenant-a").append("user_id", "new-user-id");
        Document otherUser = new Document("_id", new ObjectId()).append("customId", "tenant-a").append("user_id", "other-user");

        Document mapped = saveAsNewUserAndCaptureUpdateFilter(createdId);

        // Before the fix the exact _id condition was lost, so updateMulti matched legacyWithoutOwner and
        // ownedEarlier as well and MongoDB rejected "$set _id" on them with code 66.
        assertThat(List.of(created, legacyWithoutOwner, ownedEarlier, otherUser))
                .filteredOn(stored -> matches(mapped, stored))
                .containsExactly(created);
    }

    private Document saveAsNewUserAndCaptureUpdateFilter(ObjectId createdId) {
        MongoTemplate mongo = spy(new MongoTemplate(new SimpleMongoClientDatabaseFactory(mock(MongoClient.class), "it")));
        doAnswer(call -> {
            ApplicationEntity entity = call.getArgument(0);
            entity.setId(createdId);
            return entity;
        }).when(mongo).insert(any(ApplicationEntity.class), anyString());
        doReturn(0L).when(mongo).count(any(Query.class), eq(ApplicationEntity.class), anyString());
        doReturn(UpdateResult.acknowledged(1, 1L, null)).when(mongo)
                .updateMulti(any(Query.class), any(UpdateDefinition.class), eq(ApplicationEntity.class));
        doReturn(null).when(mongo).findOne(any(Query.class), eq(ApplicationEntity.class), anyString());

        ApplicationDto saved;
        try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            saved = controller(mongo).save(newClient()).getData();
        }

        assertThat(saved.getId()).isEqualTo(createdId);
        assertThat(saved.getClientId()).isEqualTo(createdId.toHexString());
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongo).updateMulti(captor.capture(), any(UpdateDefinition.class), eq(ApplicationEntity.class));
        return new QueryMapper(mongo.getConverter()).getMappedObject(captor.getValue().getQueryObject(),
                mongo.getConverter().getMappingContext().getPersistentEntity(ApplicationEntity.class));
    }

    /**
     * Minimal evaluator for the filter shapes produced here: field equality, {$exists: bool} and $or.
     */
    private static boolean matches(Document filter, Document stored) {
        for (Map.Entry<String, Object> condition : filter.entrySet()) {
            String key = condition.getKey();
            Object expected = condition.getValue();
            if ("$or".equals(key)) {
                boolean any = false;
                for (Object branch : (List<?>) expected) {
                    any |= matches((Document) branch, stored);
                }
                if (!any) {
                    return false;
                }
            } else if (expected instanceof Document && ((Document) expected).containsKey("$exists")) {
                if (stored.containsKey(key) != Boolean.TRUE.equals(((Document) expected).get("$exists"))) {
                    return false;
                }
            } else if (expected instanceof Document) {
                throw new IllegalArgumentException("Unsupported filter operator for " + key + ": " + expected);
            } else if (!Objects.equals(expected, stored.get(key))) {
                return false;
            }
        }
        return true;
    }

    private ApplicationController controller(MongoTemplate mongo) {
        ApplicationController controller = spy(new ApplicationController());
        ReflectionTestUtils.setField(controller, "applicationService", new ApplicationService(new ApplicationRepository(mongo)));
        UserDetail newUser = new UserDetail("new-user-id", "tenant-a", "new-user", "", List.of());
        newUser.setAuth();
        doReturn(newUser).when(controller).getLoginUser();
        return controller;
    }

    private ApplicationDto newClient() {
        ApplicationDto dto = new ApplicationDto();
        dto.setClientName("new-user-client");
        // Token/client settings are serialized by an ObjectMapper whose modules are registered in a Spring
        // @PostConstruct; pre-serialized values keep this test independent of that global initialization.
        dto.setTokenSettings("{\"@class\":\"java.util.HashMap\",\"settings.token.reuse-refresh-tokens\":true}");
        dto.setClientSettings("{\"@class\":\"java.util.HashMap\",\"settings.client.require-authorization-consent\":true}");
        return dto;
    }
}
