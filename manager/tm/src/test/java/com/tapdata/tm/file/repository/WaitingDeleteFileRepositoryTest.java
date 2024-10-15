package com.tapdata.tm.file.repository;

import org.junit.jupiter.api.Test;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/9/30 14:03
 */public class WaitingDeleteFileRepositoryTest {

    @Test
    public void testWaitingDeleteFileRepository() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoConverter converter = mock(MongoConverter.class);
        when(mongoTemplate.getConverter()).thenReturn(converter);
        MappingContext mappingContext = mock(MappingContext.class);
        when(converter.getMappingContext()).thenReturn(mappingContext);
        MongoPersistentEntity entity = mock(MongoPersistentEntity.class);
        when(mappingContext.getRequiredPersistentEntity(any(Class.class))).thenReturn(entity);
        WaitingDeleteFileRepository repository = new WaitingDeleteFileRepository(mongoTemplate);
        assertNotNull( repository);
    }
}
