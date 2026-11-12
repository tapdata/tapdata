package com.tapdata.tm.audit;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.repository.UserRepository;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.userLog.service.UserLogService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Locale;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
class AuditCredentialUpdateMongoIT {
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
