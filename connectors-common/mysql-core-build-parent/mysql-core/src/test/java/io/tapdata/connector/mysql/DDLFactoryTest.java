package io.tapdata.connector.mysql;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 21:02
 **/
public class DDLFactoryTest {
	private static KVReadOnlyMap<TapTable> tableMap;
	private static final DDLParserType DDL_PARSER_TYPE = DDLParserType.MYSQL_CCJ_SQL_PARSER;
	private static List<TapDDLEvent> tapDDLEvents;
	private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");

	@BeforeEach
	void beforeEach() {
		tableMap = new KVReadOnlyMap<TapTable>() {
			private Map<String, TapTable> map = new HashMap<String, TapTable>() {{
				TapTable tapTable = new TapTable("DDL_TEST");
				TapField field1 = new TapField("f1", "int");
				field1.setPos(1);
				field1.setPrimaryKeyPos(1);
				tapTable.putField(field1.getName(), field1);
				TapField field2 = new TapField("f2", "varchar(50)");
				field2.setPos(2);
				field2.setPrimaryKeyPos(0);
				tapTable.putField(field2.getName(), field2);
				tapTable.putField(field2.getName(), field2);
				put(tapTable.getId(), tapTable);
			}};

			@Override
			public TapTable get(String key) {
				return map.get(key);
			}
		};
		tapDDLEvents = new ArrayList<>();
	}

	@AfterEach
	void afterEach() {
		tapDDLEvents.clear();
	}

	@Test
	void addColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDL_PARSER_TYPE,
				"alter table TEST.DDL_TEST add column f1 decimal(5,2) not null comment 'test' key auto_increment",
				DDL_WRAPPER_CONFIG,
				tableMap,
				tapDDLEvents::add
		);
		Assertions.assertEquals(1, tapDDLEvents.size());
		TapDDLEvent tapDDLEvent = tapDDLEvents.get(0);
		Assertions.assertInstanceOf(TapNewFieldEvent.class, tapDDLEvent);
		Assertions.assertEquals(1, ((TapNewFieldEvent) tapDDLEvent).getNewFields().size());
		List<TapField> newFields = ((TapNewFieldEvent) tapDDLEvent).getNewFields();
		TapField tapField = newFields.get(0);
		Assertions.assertEquals("f1", tapField.getName());
		Assertions.assertEquals("decimal(5,2)", tapField.getDataType());
		Assertions.assertEquals(3, tapField.getPos());
		Assertions.assertEquals(2, tapField.getPrimaryKeyPos());
		Assertions.assertTrue(tapField.getPrimaryKey());
		Assertions.assertEquals("test", tapField.getComment());
	}

	@Test
	void changeColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDL_PARSER_TYPE,
				"alter table TEST.DDL_TEST change column f1 f1_new int(4) null comment 'test_new' unique key key",
				DDL_WRAPPER_CONFIG,
				tableMap,
				tapDDLEvents::add
		);
		Assertions.assertEquals(2, tapDDLEvents.size());
		TapDDLEvent tapDDLEvent1 = tapDDLEvents.get(0);
		Assertions.assertInstanceOf(TapAlterFieldNameEvent.class, tapDDLEvent1);
		Assertions.assertNotNull(((TapAlterFieldNameEvent) tapDDLEvent1).getNameChange());
		Assertions.assertEquals("f1", ((TapAlterFieldNameEvent) tapDDLEvent1).getNameChange().getBefore());
		Assertions.assertEquals("f1_new", ((TapAlterFieldNameEvent) tapDDLEvent1).getNameChange().getAfter());

		TapDDLEvent tapDDLEvent2 = tapDDLEvents.get(1);
		Assertions.assertEquals("int(4)", ((TapAlterFieldAttributesEvent) tapDDLEvent2).getDataTypeChange().getAfter());
		Assertions.assertTrue(((TapAlterFieldAttributesEvent) tapDDLEvent2).getNullableChange().getAfter());
		Assertions.assertEquals("test_new", ((TapAlterFieldAttributesEvent) tapDDLEvent2).getCommentChange().getAfter());
		Assertions.assertEquals(2, ((TapAlterFieldAttributesEvent) tapDDLEvent2).getPrimaryChange().getAfter());
	}

	@Test
	void dropColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDL_PARSER_TYPE,
				"alter table TEST.DDL_TEST drop column f1",
				DDL_WRAPPER_CONFIG,
				tableMap,
				tapDDLEvents::add
		);
		Assertions.assertEquals(1, tapDDLEvents.size());
		TapDDLEvent tapDDLEvent = tapDDLEvents.get(0);
		Assertions.assertInstanceOf(TapDropFieldEvent.class, tapDDLEvent);
		Assertions.assertEquals("f1", ((TapDropFieldEvent) tapDDLEvent).getFieldName());
	}

	@Test
	void modifyColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDL_PARSER_TYPE,
				"alter table TEST.DDL_TEST modify column f1 varchar(50) not null default 'test' unique key key",
				DDL_WRAPPER_CONFIG,
				tableMap,
				tapDDLEvents::add
		);
		Assertions.assertEquals(1, tapDDLEvents.size());
		TapDDLEvent tapDDLEvent = tapDDLEvents.get(0);
		Assertions.assertInstanceOf(TapAlterFieldAttributesEvent.class, tapDDLEvent);
		Assertions.assertEquals("f1", ((TapAlterFieldAttributesEvent) tapDDLEvent).getFieldName());
		Assertions.assertFalse(((TapAlterFieldAttributesEvent) tapDDLEvent).getNullableChange().getAfter());
		Assertions.assertEquals("test", ((TapAlterFieldAttributesEvent) tapDDLEvent).getDefaultChange().getAfter());
		Assertions.assertEquals(2, ((TapAlterFieldAttributesEvent) tapDDLEvent).getPrimaryChange().getAfter());
	}

	@Test
	void renameColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDL_PARSER_TYPE,
				"alter table TEST.DDL_TEST rename column f1 to f1_new",
				DDL_WRAPPER_CONFIG,
				tableMap,
				tapDDLEvents::add
		);
		Assertions.assertEquals(1, tapDDLEvents.size());
		TapDDLEvent tapDDLEvent = tapDDLEvents.get(0);
		Assertions.assertInstanceOf(TapAlterFieldNameEvent.class, tapDDLEvent);
		Assertions.assertEquals("f1", ((TapAlterFieldNameEvent) tapDDLEvent).getNameChange().getBefore());
		Assertions.assertEquals("f1_new", ((TapAlterFieldNameEvent) tapDDLEvent).getNameChange().getAfter());
	}

	@Test
	void getCapabilitiesTest() {
		List<Capability> capabilities = DDLFactory.getCapabilities(DDL_PARSER_TYPE);
		Assertions.assertEquals(4, capabilities.size());
		Assertions.assertEquals(ConnectionOptions.DDL_NEW_FIELD_EVENT, capabilities.get(0).getId());
		Assertions.assertEquals(ConnectionOptions.DDL_ALTER_FIELD_NAME_EVENT, capabilities.get(1).getId());
		Assertions.assertEquals(ConnectionOptions.DDL_ALTER_FIELD_ATTRIBUTES_EVENT, capabilities.get(2).getId());
		Assertions.assertEquals(ConnectionOptions.DDL_DROP_FIELD_EVENT, capabilities.get(3).getId());
	}
}
