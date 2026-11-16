package com.tapdata.tm.application.repository;

import com.mongodb.client.MongoClient;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ApplicationRepositoryTest {
    private MongoTemplate mongo;
    private ApplicationRepository repository;
    private UserDetail user;
    private Object previousFactory;

    @BeforeEach
    void setUp() {
        previousFactory = ReflectionTestUtils.getField(BaseRepository.class, "repositoryFactory");
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", null);
        MongoClient client = mock(MongoClient.class);
        mongo = spy(new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, "test")));
        repository = new ApplicationRepository(mongo);
        user = mock(UserDetail.class);
        when(user.getCustomerId()).thenReturn("tenant-a");
        when(user.isRoot()).thenReturn(true);
        doReturn(new AggregationResults<ApplicationEntity>(List.of(), new Document()))
                .when(mongo).aggregate(any(Aggregation.class), eq(ApplicationEntity.class), eq(ApplicationEntity.class));
    }

    @AfterEach
    void restoreRepositoryFactory() {
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", previousFactory);
    }

    @Test
    void pipeline_keeps_filters_paging_and_removes_secret_after_computing_flag() {
        Filter filter = new Filter();
        filter.where("clientName", "client-a");
        filter.setSort(List.of("clientName asc"));
        filter.setPage(2);
        filter.setSize(5);

        List<Document> pipeline = pipeline(filter);

        assertThat(pipeline).extracting(stage -> stage.keySet().iterator().next())
                .containsExactly("$match", "$sort", "$skip", "$limit", "$set", "$unset");
        assertThat(pipeline.get(0).get("$match", Document.class))
                .containsEntry("clientName", "client-a").containsEntry("customId", "tenant-a");
        assertThat(pipeline.get(1).get("$sort", Document.class)).containsEntry("clientName", 1);
        assertThat(((Number) pipeline.get(2).get("$skip")).longValue()).isEqualTo(5);
        assertThat(((Number) pipeline.get(3).get("$limit")).longValue()).isEqualTo(5);
        assertThat(pipeline.get(4).get("$set", Document.class))
                .isEqualTo(new Document("computedClientSecretConfigured",
                        new Document("$ne", List.of(new Document("$ifNull", List.of("$clientSecret", "")), ""))));
        assertThat(pipeline.get(5)).isEqualTo(new Document("$unset", "clientSecret"));
    }

    @Test
    void inclusion_projection_returns_flag_but_not_secret() {
        Field fields = new Field();
        fields.put("clientName", true);
        fields.put("clientSecret", true);
        assertProjection(fields, new Document("clientName", 1).append("computedClientSecretConfigured", 1));
    }

    @Test
    void id_only_and_secret_only_projections_still_return_flag() {
        Field id = new Field();
        id.put("_id", true);
        assertProjection(id, new Document("_id", 1).append("computedClientSecretConfigured", 1));
        Field secret = new Field();
        secret.put("clientSecret", true);
        assertProjection(secret, new Document("computedClientSecretConfigured", 1));
    }

    @Test
    void exclusion_projection_retains_computed_flag_without_exposing_secret() {
        Field fields = new Field();
        fields.put("clientName", false);
        fields.put("clientSecret", false);
        assertProjection(fields, new Document("clientName", 0));
    }

    private void assertProjection(Field fields, Document expected) {
        Filter filter = new Filter();
        filter.setFields(fields);
        List<Document> pipeline = pipeline(filter);
        int unsetIndex = pipeline.size() - 2;
        assertThat(pipeline.get(unsetIndex)).isEqualTo(new Document("$unset", "clientSecret"));
        assertThat(pipeline.get(unsetIndex + 1)).isEqualTo(new Document("$project", expected));
    }

    private List<Document> pipeline(Filter filter) {
        try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            repository.findWithoutClientSecrets(filter, user);
        }
        ArgumentCaptor<Aggregation> captor = ArgumentCaptor.forClass(Aggregation.class);
        verify(mongo, atLeastOnce()).aggregate(captor.capture(), eq(ApplicationEntity.class), eq(ApplicationEntity.class));
        List<Aggregation> invocations = captor.getAllValues();
        return invocations.get(invocations.size() - 1).toPipeline(Aggregation.DEFAULT_CONTEXT);
    }
}
