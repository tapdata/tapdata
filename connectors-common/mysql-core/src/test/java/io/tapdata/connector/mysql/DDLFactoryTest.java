package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.ddl.DDLFactory;
import io.tapdata.connector.mysql.ddl.DDLParserType;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

	@BeforeAll
	static void beforeAll() {
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
	}

	@Test
	void addColumnWrapperTest() throws Throwable {
		DDLFactory.ddlToTapDDLEvent(
				DDLParserType.CCJ_SQL_PARSER,
				"alter table TEST.DDL_TEST add column f1 decimal(5,2) not null comment 'test' key auto_increment",
				tableMap,
				tapDDLEvent -> {
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
				});
	}

	@Test
	void changeColumnWrapperTest() throws Throwable {
		List<TapDDLEvent> tapDDLEvents = new ArrayList<>();
		DDLFactory.ddlToTapDDLEvent(
				DDLParserType.CCJ_SQL_PARSER,
				"alter table TEST.DDL_TEST change column f1 f1_new int(4) null comment 'test_new' unique key key",
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
		Assertions.assertTrue(((TapAlterFieldAttributesEvent) tapDDLEvent2).getNotNullChange().getAfter());
		Assertions.assertEquals("test_new", ((TapAlterFieldAttributesEvent) tapDDLEvent2).getCommentChange().getAfter());
		Assertions.assertEquals(2, ((TapAlterFieldAttributesEvent) tapDDLEvent2).getPrimaryChange().getAfter());
	}
}
