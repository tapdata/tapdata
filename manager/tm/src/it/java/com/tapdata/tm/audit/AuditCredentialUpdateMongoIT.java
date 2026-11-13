package com.tapdata.tm.audit;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.repository.UserRepository;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.userLog.service.UserLogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Locale;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
class AuditCredentialUpdateMongoIT {
    @Test
    void list_computes_configuration_in_mongo_without_returning_secrets() throws Exception {
        try (MongoClient client = MongoClients.create(System.getProperty("audit.it.mongoUri"));
             MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class)) {
            MongoTemplate mongo = new MongoTemplate(client, "audit_credential_list_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                String collection = mongo.getCollectionName(ApplicationEntity.class);
                mongo.getCollection(collection).insertMany(List.of(
                        new Document("_id", new ObjectId()).append("clientName", "configured").append("clientSecret", "private-value").append("customId", "tenant-a"),
                        new Document("_id", new ObjectId()).append("clientName", "empty").append("clientSecret", "").append("customId", "tenant-a"),
                        new Document("_id", new ObjectId()).append("clientName", "missing").append("customId", "tenant-a"),
                        new Document("_id", new ObjectId()).append("clientName", "other-tenant").append("clientSecret", "private-value").append("customId", "tenant-b")));
                ApplicationRepository repository = new ApplicationRepository(mongo);
                UserDetail login = mock(UserDetail.class);
                when(login.getCustomerId()).thenReturn("tenant-a");
                when(login.isRoot()).thenReturn(true);
                Filter filter = new Filter();
                List<ApplicationEntity> results = repository.findWithoutClientSecrets(filter, login);
                assertThat(results).hasSize(3).allSatisfy(item -> assertThat(item.getClientSecret()).isNull());
                assertThat(results).filteredOn(item -> "configured".equals(item.getClientName()))
                        .singleElement().extracting(ApplicationEntity::getComputedClientSecretConfigured).isEqualTo(true);
                assertThat(results).filteredOn(item -> !"configured".equals(item.getClientName()))
                        .allSatisfy(item -> assertThat(item.getComputedClientSecretConfigured()).isFalse());
                ApplicationService service = new ApplicationService(repository);
                ApplicationDto dto = service.convertToDto(results.stream()
                        .filter(item -> "configured".equals(item.getClientName())).findFirst().orElseThrow(), ApplicationDto.class);
                assertThat(dto.isClientSecretConfigured()).isTrue();
                assertThat(new ObjectMapper().writeValueAsString(dto)).contains("\"clientSecretConfigured\":true")
                        .doesNotContain("private-value", "computedClientSecretConfigured", "\"clientSecret\":");
                RoleRepository roles = mock(RoleRepository.class);
                when(roles.findAll(any(Query.class))).thenReturn(List.of());
                try (MockedStatic<SpringContextHelper> context = mockStatic(SpringContextHelper.class)) {
                    context.when(() -> SpringContextHelper.getBean(RoleRepository.class)).thenReturn(roles);
                    assertThat(service.find(null, login).getTotal()).isEqualTo(3);
                    filter.setSort(List.of("clientName asc"));
                    filter.setSize(1);
                    filter.setPage(2);
                    var page = service.find(filter, login);
                    assertThat(page.getTotal()).isEqualTo(3);
                    assertThat(page.getItems()).hasSize(1);
                    assertThat(((ApplicationDto) page.getItems().get(0)).getClientName()).isEqualTo("empty");
                    assertThat(((ApplicationDto) page.getItems().get(0)).isClientSecretConfigured()).isFalse();
                    filter.setSize(20);
                    filter.setPage(1);
                }
                Field fields = new Field();
                fields.put("clientName", true);
                filter.setFields(fields);
                assertThat(repository.findWithoutClientSecrets(filter, login)).hasSize(3)
                        .allSatisfy(item -> {
                            assertThat(item.getClientSecret()).isNull();
                            assertThat(item.getComputedClientSecretConfigured()).isEqualTo("configured".equals(item.getClientName()));
                        });
                fields.clear();
                fields.put("clientName", false);
                assertThat(repository.findWithoutClientSecrets(filter, login)).hasSize(3)
                        .allSatisfy(item -> {
                            assertThat(item.getClientName()).isNull();
                            assertThat(item.getClientSecret()).isNull();
                            assertThat(item.getComputedClientSecretConfigured()).isNotNull();
                        });
                fields.clear();
                fields.put("clientSecret", true);
                assertThat(repository.findWithoutClientSecrets(filter, login)).hasSize(3)
                        .allSatisfy(item -> {
                            assertThat(item.getClientName()).isNull();
                            assertThat(item.getClientSecret()).isNull();
                            assertThat(item.getComputedClientSecretConfigured()).isNotNull();
                        });
            } finally {
                mongo.getDb().drop();
            }
        }
    }

    @Test
    void editing_other_fields_preserves_credentials_and_explicit_replacement_works() {
        try (MongoClient client = MongoClients.create(System.getProperty("audit.it.mongoUri"))) {
            MongoTemplate mongo = new MongoTemplate(client, "audit_credential_edit_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                ObjectId clientId = new ObjectId();
                String appCollection = mongo.getCollectionName(ApplicationEntity.class);
                mongo.getCollection(appCollection).insertOne(new Document("_id", clientId)
                        .append("clientName", "before").append("clientSecret", "original-client-secret"));
                ApplicationService applications = new ApplicationService(new ApplicationRepository(mongo));
                UserDetail login = mock(UserDetail.class);
                when(login.isRoot()).thenReturn(true);
                ApplicationDto update = new ApplicationDto();
                update.setId(clientId);
                update.setClientName("after");
                ApplicationDto updated = applications.updateById(update, login);
                assertThat(updated.isClientSecretConfigured()).isTrue();
                Document storedApp = mongo.getCollection(appCollection).find(new Document("_id", clientId)).first();
                assertThat(storedApp.getString("clientName")).isEqualTo("after");
                assertThat(storedApp.getString("clientSecret")).isEqualTo("original-client-secret");
                update.setClientSecret("replacement-client-secret");
                applications.updateById(update, login);
                storedApp = mongo.getCollection(appCollection).find(new Document("_id", clientId)).first();
                assertThat(storedApp.getString("clientSecret")).isEqualTo("replacement-client-secret");

                ObjectId userId = new ObjectId();
                String userCollection = mongo.getCollectionName(User.class);
                mongo.getCollection(userCollection).insertOne(new Document("_id", userId)
                        .append("username", "before").append("accesscode", "original-access-code"));
                UserServiceImpl users = new UserServiceImpl(new UserRepository(mongo));
                ReflectionTestUtils.setField(users, "userLogService", mock(UserLogService.class));
                when(login.getUserId()).thenReturn(userId.toHexString());
                users.updateUserSetting(userId.toHexString(), "{\"username\":\"after\"}", login, Locale.ROOT, true);
                Document storedUser = mongo.getCollection(userCollection).find(new Document("_id", userId)).first();
                assertThat(storedUser.getString("username")).isEqualTo("after");
                assertThat(storedUser.getString("accesscode")).isEqualTo("original-access-code");
                users.updateUserSetting(userId.toHexString(), "{\"accesscode\":\"replacement-access-code\"}", login, Locale.ROOT, true);
                storedUser = mongo.getCollection(userCollection).find(new Document("_id", userId)).first();
                assertThat(storedUser.getString("accesscode")).isEqualTo("replacement-access-code");
            } finally {
                mongo.getDb().drop();
            }
        }
    }
}
