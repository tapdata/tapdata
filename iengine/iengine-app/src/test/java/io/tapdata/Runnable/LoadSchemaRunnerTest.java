package io.tapdata.Runnable;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.Connections;
import com.tapdata.entity.LoadSchemaProgress;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import io.tapdata.utils.UnitTestUtils;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/7/24 10:45 Create
 */
class LoadSchemaRunnerTest {
    LoadSchemaRunner runner;
    long lastUpdate;
    String schemaVersion;
    LoadSchemaProgress loadSchemaProgress;
    Connections connections;
    ClientMongoOperator clientMongoOperator;

    @BeforeEach
    void setUp() {
        runner = mock(LoadSchemaRunner.class);

        lastUpdate = System.currentTimeMillis();
        UnitTestUtils.injectField(LoadSchemaRunner.class, runner, "lastUpdate", lastUpdate);
        schemaVersion = UUIDGenerator.uuid();
        UnitTestUtils.injectField(LoadSchemaRunner.class, runner, "schemaVersion", schemaVersion);
        loadSchemaProgress = mock(LoadSchemaProgress.class);
        UnitTestUtils.injectField(LoadSchemaRunner.class, runner, "loadSchemaProgress", loadSchemaProgress);
        connections = mock(Connections.class);
        UnitTestUtils.injectField(LoadSchemaRunner.class, runner, "connections", connections);
        clientMongoOperator = mock(ClientMongoOperator.class);
        UnitTestUtils.injectField(LoadSchemaRunner.class, runner, "clientMongoOperator", clientMongoOperator);
    }

    @Nested
    class UpdateConnections2LoadingTest {

        @Test
        void testFullUpdate() {
            doCallRealMethod().when(runner).updateConnections2Loading(connections);

            assertFalse(runner.updateConnections2Loading(connections));
            verify(runner, times(1)).updateConnections(any());
        }

        @Test
        void testPartialUpdateAndReturnFalse() {
            String connId = "test-conn-id";
            String oldSchemaVersion = "test-old-version";
            String partialUpdateFilter = "test-partial-update-filter";
            String loadFieldsStatus = DataSourceConnectionDto.LOAD_FIELD_STATUS_LOADING;

            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getMatchedCount()).thenReturn(1L);
            when(connections.getId()).thenReturn(connId);
            when(connections.getPartialUpdateWithSchemaVersion()).thenReturn(oldSchemaVersion);
            when(connections.getPartialUpdateFilter()).thenReturn(partialUpdateFilter);
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("_id"));
                assertEquals(oldSchemaVersion, doc.get(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(loadFieldsStatus, doc.getString(DataSourceConnectionDto.FIELD_LOAD_FIELDS_STATUS));
                assertEquals(partialUpdateFilter, doc.getString(DataSourceConnectionDto.FIELD_PARTIAL_UPDATE_FILTER));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                assertEquals(schemaVersion, doc.getString(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                return updateResult;
            }).when(clientMongoOperator).update(any(Query.class), any(Update.class), anyString());
            doCallRealMethod().when(runner).updateConnections2Loading(eq(connections));

            assertFalse(runner.updateConnections2Loading(connections));
            verify(clientMongoOperator, times(1)).update(any(Query.class), any(Update.class), anyString());
        }

        @Test
        void testPartialUpdateAndNullFilterAndReturnFalse() {
            String connId = "test-conn-id";
            String oldSchemaVersion = "test-old-version";
            String loadFieldsStatus = DataSourceConnectionDto.LOAD_FIELD_STATUS_LOADING;

            UpdateResult updateResult = mock(UpdateResult.class);
            when(connections.getId()).thenReturn(connId);
            when(connections.getPartialUpdateWithSchemaVersion()).thenReturn(oldSchemaVersion);
            doAnswer(invocation -> {
                Document doc = invocation.<Query>getArgument(0).getQueryObject();
                assertEquals(connId, doc.getString("_id"));
                assertEquals(oldSchemaVersion, doc.get(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));

                doc = invocation.<Update>getArgument(1).getUpdateObject().get("$set", Document.class);
                assertEquals(loadFieldsStatus, doc.getString(DataSourceConnectionDto.FIELD_LOAD_FIELDS_STATUS));
                assertNull(doc.getString(DataSourceConnectionDto.FIELD_PARTIAL_UPDATE_FILTER));
                assertEquals(lastUpdate, doc.getLong(DataSourceConnectionDto.FIELD_LAST_UPDATE));
                assertEquals(schemaVersion, doc.getString(DataSourceConnectionDto.FIELD_SCHEMA_VERSION));
                return updateResult;
            }).when(clientMongoOperator).update(any(Query.class), any(Update.class), anyString());
            doCallRealMethod().when(runner).updateConnections2Loading(eq(connections));

            assertTrue(runner.updateConnections2Loading(connections));
            verify(clientMongoOperator, times(1)).update(any(Query.class), any(Update.class), anyString());
        }
    }
}
