package com.tapdata.tm.metadatadefinition.repository;

import com.tapdata.tm.config.security.UserDetail;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class MetadataDefinitionRepositoryTest {
    MetadataDefinitionRepository repository;
    @BeforeEach
    void setUp() {
        repository = mock(MetadataDefinitionRepository.class);
    }
    @Nested
    class applyUserDetail {
        Query query;
        UserDetail userDetail;
        @BeforeEach
        void beforeEach() {
            query = new Query();
            userDetail = mock(UserDetail.class);
            doCallRealMethod().when(repository).applyUserDetail(query, userDetail);
        }
        @Test
        void test() {
            when(userDetail.isRoot()).thenReturn(false);
            Query actual = repository.applyUserDetail(query, userDetail);
            Document document = new Document();
            document.put("customId", userDetail.getCustomerId());
            document.put("user_id", userDetail.getCustomerId());
            assertEquals(document, actual.getQueryObject());
        }
    }
}
