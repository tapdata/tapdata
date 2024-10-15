package com.tapdata.tm.schedule;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import com.tapdata.tm.file.entity.WaitingDeleteFile;
import com.tapdata.tm.file.repository.WaitingDeleteFileRepository;
import com.tapdata.tm.file.service.FileService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/2 18:42
 */
@ExtendWith(MockitoExtension.class)
public class DeleteFileScheduleTest {

    @Mock
    private FileService fileService;
    @Mock
    private WaitingDeleteFileRepository waitingDeleteFileRepository;

    @Test
    public void testCleanUpForDatabaseTypes() {

        assertNotNull(fileService);
        assertNotNull(waitingDeleteFileRepository);
        fileService.setWaitingDeleteFileRepository(waitingDeleteFileRepository);

        DeleteFileSchedule deleteFileSchedule = new DeleteFileSchedule();
        deleteFileSchedule.setFileService(fileService);

        assertDoesNotThrow(deleteFileSchedule::cleanUpForDatabaseTypes);

        verify(fileService, times(1)).cleanupWaitingDeleteFiles();

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
				public int available() {
						return 0;
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
