package com.tapdata.tm.application.repository;

import com.mongodb.client.MongoClient;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ApplicationRepositoryTest {
    private MongoTemplate mongo;
    private ApplicationRepository repository;
    private Object previousFactory;

    @BeforeEach
    void setUp() {
        previousFactory = ReflectionTestUtils.getField(BaseRepository.class, "repositoryFactory");
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", null);
        MongoClient client = mock(MongoClient.class);
        mongo = spy(new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, "test")));
        repository = new ApplicationRepository(mongo);
    }

    @AfterEach
    void restoreRepositoryFactory() {
        ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", previousFactory);
    }


    @Test
    void nonAdmin_applyUserDetail_keeps_exact_id_filter() {
        ObjectId id = new ObjectId();
        UserDetail nonAdmin = nonAdminUser();
        Query query = Query.query(Criteria.where("id").is(id.toHexString()));

        try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            repository.applyUserDetail(query, nonAdmin);
        }

        Document mapped = mapped(query);
        assertThat(mapped.get("_id")).isEqualTo(id);
        assertThat(mapped).containsEntry("customId", "tenant-a");
        assertThat(mapped.getList("$or", Document.class)).containsExactly(
                new Document("user_id", "user-a"),
                new Document("user_id", new Document("$exists", false)));
    }

    @Test
    void nonAdmin_findById_does_not_duplicate_id_criteria() {
        ObjectId id = new ObjectId();
        UserDetail nonAdmin = nonAdminUser();
        doReturn(null).when(mongo).findOne(any(Query.class), eq(ApplicationEntity.class), anyString());

        try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
            assertThat(repository.findById(id, nonAdmin)).isEmpty();
        }

        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongo).findOne(captor.capture(), eq(ApplicationEntity.class), anyString());
        Document mapped = mapped(captor.getValue());
        assertThat(mapped.get("_id")).isEqualTo(id);
        assertThat(mapped.getList("$or", Document.class)).hasSize(2);
    }

    private UserDetail nonAdminUser() {
        UserDetail nonAdmin = new UserDetail("user-a", "tenant-a", "new-user", "", List.of());
        nonAdmin.setAuth();
        return nonAdmin;
    }

    private Document mapped(Query query) {
        return new QueryMapper(mongo.getConverter()).getMappedObject(query.getQueryObject(),
                mongo.getConverter().getMappingContext().getPersistentEntity(ApplicationEntity.class));
    }

}
