package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}