package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import io.tapdata.pdk.apis.entity.Capability;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-10 19:21
 **/
@DisplayName("Class ConnHeartbeatUtils Test")
class ConnHeartbeatUtilsTest {

    @Test
    @DisplayName("Method generateConnections test")
    void testGenerateConnections() {
        DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
        String testId = "test_id";
        String type = "test_type";
        String pdkHash = "test_pdk_hash";
        dataSourceDefinitionDto.setType(type);
        dataSourceDefinitionDto.setPdkHash(pdkHash);

        DataSourceConnectionDto dataSourceConnectionDto = ConnHeartbeatUtils.generateConnections(testId, dataSourceDefinitionDto);

        assertNotNull(dataSourceConnectionDto);
        assertEquals(ConnHeartbeatUtils.CONNECTION_NAME, dataSourceConnectionDto.getName());
        assertEquals(DataSourceConnectionDto.STATUS_READY, dataSourceConnectionDto.getStatus());
        assertEquals("source", dataSourceConnectionDto.getConnection_type());
        assertEquals(CreateTypeEnum.System, dataSourceConnectionDto.getCreateType());
        assertEquals(dataSourceDefinitionDto.getType(), dataSourceConnectionDto.getDatabase_type());
        assertEquals(DataSourceDefinitionDto.PDK_TYPE, dataSourceConnectionDto.getPdkType());
        assertEquals(dataSourceDefinitionDto.getPdkHash(), dataSourceConnectionDto.getPdkHash());
        assertEquals(0, dataSourceConnectionDto.getRetry());
        assertEquals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), dataSourceConnectionDto.getAccessNodeType());
        Map<String, Object> config = dataSourceConnectionDto.getConfig();
        assertNotNull(config);
        assertEquals(ConnHeartbeatUtils.MODE, config.get("mode"));
        assertEquals(1, config.get("initial_totals"));
        assertEquals(1000, config.get("incremental_interval"));
        assertEquals(1, config.get("incremental_interval_totals"));
        Object incrementalTypes = config.get("incremental_types");
        assertInstanceOf(int[].class, incrementalTypes);
        assertEquals(1, ((int[]) incrementalTypes).length);
        assertEquals(1, ((int[]) incrementalTypes)[0]);
        assertEquals(ConnHeartbeatUtils.TABLE_NAME, config.get("table_name"));
        Object tableFieldsObj = config.get("table_fields");
        assertInstanceOf(ArrayList.class, tableFieldsObj);
        List<?> tableFields = (ArrayList<?>) tableFieldsObj;
        assertEquals(2, tableFields.size());
        Object obj1 = tableFields.get(0);
        assertInstanceOf(HashMap.class, obj1);
        Map<?, ?> map1 = (HashMap<?, ?>) obj1;
        assertTrue((Boolean) map1.get("pri"));
        assertEquals("id", map1.get("name"));
        assertEquals("string(64)", map1.get("type"));
        Object obj2 = tableFields.get(1);
        assertInstanceOf(HashMap.class, obj2);
        Map<?, ?> map2 = (HashMap<?, ?>) obj2;
        assertFalse((Boolean) map2.get("pri"));
        assertEquals("ts", map2.get("name"));
        assertEquals("now", map2.get("type"));
    }

    @Nested
    class HasNoHeartbeatTagTest {
        @Test
        void testNullTags() {
            DataSourceConnectionDto connectionDto = Mockito.mock(DataSourceConnectionDto.class);
            Mockito.when(connectionDto.getDefinitionTags()).thenReturn(null);
            Assertions.assertFalse(ConnHeartbeatUtils.hasNoHeartbeatTag(connectionDto));
        }

        @Test
        void testEmptyTags() {
            DataSourceConnectionDto connectionDto = Mockito.mock(DataSourceConnectionDto.class);
            Mockito.when(connectionDto.getDefinitionTags()).thenReturn(new ArrayList<>());
            Assertions.assertFalse(ConnHeartbeatUtils.hasNoHeartbeatTag(connectionDto));
        }

        @Test
        void testReturnTrue() {
            DataSourceConnectionDto connectionDto = Mockito.mock(DataSourceConnectionDto.class);
            Mockito.when(connectionDto.getDefinitionTags()).thenReturn(Collections.singletonList(ConnHeartbeatUtils.CONNECTOR_TAGS_NO_HEARTBEAT));
            Assertions.assertTrue(ConnHeartbeatUtils.hasNoHeartbeatTag(connectionDto));
        }
    }

    @Nested
    class CheckConnectionTest {

        DataSourceConnectionDto connectionDto;
        List<Capability> capabilities;

        @BeforeEach
        void setUp() {
            capabilities = new ArrayList<>();
            capabilities.add(new Capability(CapabilityEnum.STREAM_READ_FUNCTION.name()));
            capabilities.add(new Capability(CapabilityEnum.CREATE_TABLE_FUNCTION.name()));
            capabilities.add(new Capability(CapabilityEnum.CREATE_TABLE_V2_FUNCTION.name()));
            capabilities.add(new Capability(CapabilityEnum.WRITE_RECORD_FUNCTION.name()));
            capabilities.add(new Capability(CapabilityEnum.DROP_TABLE_FUNCTION.name()));

            connectionDto = new DataSourceConnectionDto();
            connectionDto.setHeartbeatEnable(true);
            connectionDto.setCapabilities(capabilities);
            connectionDto.setDatabase_type(String.format("not-%s", ConnHeartbeatUtils.PDK_NAME));
            connectionDto.setConnection_type("source_and_target");
        }

        @Test
        void testDisableHeartbeats() {
            connectionDto.setHeartbeatEnable(false);
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testDummyConnector() {
            connectionDto.setDatabase_type(ConnHeartbeatUtils.PDK_NAME);
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testNullCapabilities() {
            connectionDto.setCapabilities(null);
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testSourceConnection() {
            connectionDto.setConnection_type("source");
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testTargetConnection() {
            connectionDto.setConnection_type("target");
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testHasNoHeartbeatTag() {
            connectionDto.setDefinitionTags(Collections.singletonList(ConnHeartbeatUtils.CONNECTOR_TAGS_NO_HEARTBEAT));
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testNoStreamReadFunction() {
            capabilities.removeIf(capability -> capability.getId().equals(CapabilityEnum.STREAM_READ_FUNCTION.name()));
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testNoCreateTableFunction() {
            capabilities.removeIf(capability -> capability.getId().equals(CapabilityEnum.CREATE_TABLE_FUNCTION.name()));
            capabilities.removeIf(capability -> capability.getId().equals(CapabilityEnum.CREATE_TABLE_V2_FUNCTION.name()));
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testNoWriteRecordFunction() {
            capabilities.removeIf(capability -> capability.getId().equals(CapabilityEnum.WRITE_RECORD_FUNCTION.name()));
            Assertions.assertFalse(ConnHeartbeatUtils.checkConnection(connectionDto));
        }

        @Test
        void testTrue() {
            Assertions.assertTrue(ConnHeartbeatUtils.checkConnection(connectionDto));
        }
    }
}
