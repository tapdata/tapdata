package com.tapdata.tm.schedule;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import com.tapdata.tm.file.service.FileService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/2 18:42
 */
@ExtendWith(MockitoExtension.class)
public class DeleteFileScheduleTest {

    @Mock
    private DataSourceDefinitionRepository dataSourceDefinitionRepository;
    @Mock
    private FileService fileService;
    @Mock
    private MongoCollection<Document> collection;
    @Mock
    private MongoTemplate mongoOperations;
    @Mock
    private FindIterable<DataSourceDefinitionDto> findIterable;

    @Test
    public void testCleanUpForDatabaseTypes() {

        assertNotNull(fileService);
        assertNotNull(dataSourceDefinitionRepository);
        assertNotNull(collection);
        assertNotNull(mongoOperations);
        assertNotNull(findIterable);

        List<ObjectId> originalIds = Arrays.asList(
                new ObjectId("66ace100fb85b005263f2c13"),
                new ObjectId("66ace100fb85b005263f2c14"),
                new ObjectId("66ace100fb85b005263f2c15"),
                new ObjectId("66ace100fb85b005263f2c16"),
                new ObjectId("66ace100fb85b005263f2c17")
        );

        DeleteFileSchedule deleteFileSchedule = new DeleteFileSchedule();
        deleteFileSchedule.setFileService(fileService);
        deleteFileSchedule.setDataSourceDefinitionRepository(dataSourceDefinitionRepository);

        List<DataSourceDefinitionDto> result = new ArrayList<>();
        DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
        dto.setId(new ObjectId());
        dto.setJarRid(originalIds.get(0).toHexString());
        dto.setIcon(originalIds.get(1).toHexString());

        LinkedHashMap<String, Object> messages = new LinkedHashMap<>();
        dto.setMessages(messages);
        messages.put("zh_CN", new HashMap<String, Object>(){{
            put("doc", originalIds.get(2));
        }});
        messages.put("zh_TW", new HashMap<String, Object>(){{
            put("doc", originalIds.get(3).toHexString());
        }});
        messages.put("en_US", new HashMap<String, Object>(){{
            put("doc", originalIds.get(4));
        }});
        messages.put("en_DE", new HashMap<String, Object>());
        messages.put("en_UE", new HashMap<String, Object>(){{
            put("doc", true);
        }});
        messages.put("test", new ArrayList<>());

        result.add(dto);

        when(dataSourceDefinitionRepository.getMongoOperations()).thenReturn(mongoOperations);
        when(mongoOperations.getCollection(PkdSourceService.DATABASE_TYPES_WAITING_DELETED_COLLECTION_NAME))
                .thenReturn(collection);
        when(collection.find(DataSourceDefinitionDto.class)).thenReturn(findIterable);

        when(findIterable.cursor()).thenReturn(new MockMongoCursor<>(Collections.emptyList()));
        assertDoesNotThrow(deleteFileSchedule::cleanUpForDatabaseTypes);
        verify(fileService, never()).deleteFileById(any());

        when(findIterable.cursor()).thenReturn(new MockMongoCursor<>(result));
        doNothing().when(fileService).deleteFileById(any());

        assertDoesNotThrow(deleteFileSchedule::cleanUpForDatabaseTypes);

        ArgumentCaptor<ObjectId> captor = ArgumentCaptor.forClass(ObjectId.class);

        verify(fileService, times(5)).deleteFileById(captor.capture());

        List<ObjectId> ids = captor.getAllValues().stream().sorted().collect(Collectors.toList());

        assertEquals(originalIds.size(), ids.size());
        assertTrue(ids.containsAll(originalIds));
        assertTrue(originalIds.containsAll(ids));

    }

    private static class MockMongoCursor<T> implements MongoCursor<T> {
        private final List<T> result;

        public MockMongoCursor(List<T> result) {
            this.result = result;
        }

        private int readIndex = -1;
        @Override
        public void close() {

        }

        @Override
        public boolean hasNext() {
            return result.size() > readIndex + 1;
        }

        @Override
        public T next() {
            readIndex++;
            return result.get(readIndex);
        }

        @Override
        public T tryNext() {
            if (hasNext()) {
                return next();
            }
            return null;
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }

    }
}
