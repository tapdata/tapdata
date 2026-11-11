package com.tapdata.tm.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.application.controller.ApplicationController;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@EnabledIfSystemProperty(named = "audit.it.mongoUri", matches = ".+")
class AuditClientProjectionMongoIT {
    @Test
    void clientProjectionsReportConfigurationWithoutReturningSecret() throws Exception {
        try (MongoClient client = MongoClients.create(System.getProperty("audit.it.mongoUri"));
             MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class);
             MockedStatic<SpringContextHelper> context = mockStatic(SpringContextHelper.class)) {
            MongoTemplate mongo = new MongoTemplate(client, "audit_client_projection_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                ObjectId configured = new ObjectId();
                String collection = mongo.getCollectionName(ApplicationEntity.class);
                mongo.getCollection(collection).insertOne(new Document("_id", configured)
                        .append("customId", "tenant-a").append("clientName", "configured")
                        .append("clientSecret", "test-secret"));
                UserDetail login = mock(UserDetail.class);
                when(login.getCustomerId()).thenReturn("tenant-a");
                when(login.getAuthorities()).thenReturn(Collections.emptyList());
                when(login.isRoot()).thenReturn(true);
                RoleRepository roles = mock(RoleRepository.class);
                when(roles.findAll(any(Query.class))).thenReturn(Collections.emptyList());
                context.when(() -> SpringContextHelper.getBean(RoleRepository.class)).thenReturn(roles);
                permissions.when(() -> DataPermissionHelper.check(any(), any(), any(), any(), isNull(), any(), any()))
                        .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
                permissions.when(() -> DataPermissionHelper.checkOfQuery(any(), any(), any(), any(), any(), any(), any()))
                        .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
                ApplicationController controller = spy(new ApplicationController());
                ReflectionTestUtils.setField(controller, "applicationService", new ApplicationService(new ApplicationRepository(mongo)));
                doReturn(login).when(controller).getLoginUser();
                MockMvc http = MockMvcBuilders.standaloneSetup(controller).build();
                ObjectMapper json = new ObjectMapper();
                for (String fields : new String[]{"{\"clientName\":true}", "{\"clientSecret\":false}"}) {
                    String listing = http.perform(get("/api/Applications").param("filter", "{\"fields\":" + fields + "}"))
                            .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
                    JsonNode item = json.readTree(listing).path("data").path("items").get(0);
                    assertThat(item.path("clientSecretConfigured").asBoolean()).isTrue();
                    assertThat(listing).doesNotContain("test-secret", "\"clientSecret\":");

                    String detail = http.perform(get("/api/Applications/{id}", configured.toHexString()).param("fields", fields))
                            .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
                    assertThat(json.readTree(detail).path("data").path("clientSecretConfigured").asBoolean()).isTrue();
                    assertThat(detail).doesNotContain("test-secret", "\"clientSecret\":");
                }
                ObjectId unconfigured = new ObjectId();
                mongo.getCollection(collection).insertOne(new Document("_id", unconfigured)
                        .append("customId", "tenant-a").append("clientName", "unconfigured"));
                String empty = http.perform(get("/api/Applications/{id}", unconfigured.toHexString())
                                .param("fields", "{\"clientName\":true}"))
                        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
                assertThat(json.readTree(empty).path("data").path("clientSecretConfigured").asBoolean()).isFalse();
            } finally {
                mongo.getDb().drop();
            }
        }
    }
}
