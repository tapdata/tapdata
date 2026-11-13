package com.tapdata.tm.application;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.UUID;

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
        MongoTemplate mongo = spy(new MongoTemplate(new SimpleMongoClientDatabaseFactory(mock(MongoClient.class), "it")));
        ObjectId createdId = new ObjectId();
        doAnswer(call -> {
            ApplicationEntity entity = call.getArgument(0);
            entity.setId(createdId);
            return entity;
        }).when(mongo).insert(any(ApplicationEntity.class), anyString());
        doReturn(0L).when(mongo).count(any(Query.class), eq(ApplicationEntity.class), anyString());
        doReturn(UpdateResult.acknowledged(1, 1L, null)).when(mongo)
                .updateMulti(any(Query.class), any(UpdateDefinition.class), eq(ApplicationEntity.class));
        doReturn(null).when(mongo).findOne(any(Query.class), eq(ApplicationEntity.class), anyString());
        ApplicationController controller = controller(mongo);

        ApplicationDto created;
        try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            created = controller.save(newClient()).getData();
        }

        assertThat(created.getId()).isEqualTo(createdId);
        assertThat(created.getClientId()).isEqualTo(createdId.toHexString());
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongo).updateMulti(captor.capture(), any(UpdateDefinition.class), eq(ApplicationEntity.class));
        Document mapped = new QueryMapper(mongo.getConverter()).getMappedObject(captor.getValue().getQueryObject(),
                mongo.getConverter().getMappingContext().getPersistentEntity(ApplicationEntity.class));
        assertThat(mapped.get("_id")).isEqualTo(createdId);
        assertThat(mapped).containsEntry("customId", "tenant-a");
        assertThat(mapped.getList("$or", Document.class)).containsExactly(
                new Document("user_id", "new-user-id"),
                new Document("user_id", new Document("$exists", false)));
    }

    @Test
    @EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
    @DisplayName("非管理员创建 API Client：真实 MongoDB 不再报 _id 不可变错误（code 66）")
    void should_not_raise_immutable_id_error_on_real_mongo() {
        try (MongoClient client = MongoClients.create(System.getProperty("audit.it.mongoUri"));
             MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            MongoTemplate mongo = new MongoTemplate(client, "tap13258_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                String collection = mongo.getCollectionName(ApplicationEntity.class);
                ObjectId legacy = new ObjectId();
                ObjectId ownedEarlier = new ObjectId();
                mongo.getCollection(collection).insertMany(List.of(
                        new Document("_id", legacy).append("customId", "tenant-a").append("clientName", "legacy-without-owner")
                                .append("clientId", "legacy-client"),
                        new Document("_id", ownedEarlier).append("customId", "tenant-a").append("clientName", "owned-earlier")
                                .append("clientId", "owned-client").append("user_id", "new-user-id")));

                ApplicationDto created = controller(mongo).save(newClient()).getData();

                assertThat(created.getId()).isNotNull();
                assertThat(created.getClientId()).isEqualTo(created.getId().toHexString());
                Document stored = mongo.getCollection(collection).find(new Document("_id", created.getId())).first();
                assertThat(stored).isNotNull();
                assertThat(stored.getString("clientId")).isEqualTo(created.getId().toHexString());
                assertThat(stored.getString("user_id")).isEqualTo("new-user-id");
                assertThat(mongo.getCollection(collection).countDocuments()).isEqualTo(3);
                assertThat(mongo.getCollection(collection).find(new Document("_id", legacy)).first())
                        .containsEntry("clientName", "legacy-without-owner").containsEntry("clientId", "legacy-client");
                assertThat(mongo.getCollection(collection).find(new Document("_id", ownedEarlier)).first())
                        .containsEntry("clientName", "owned-earlier").containsEntry("clientId", "owned-client");
            } finally {
                mongo.getDb().drop();
            }
        }
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
