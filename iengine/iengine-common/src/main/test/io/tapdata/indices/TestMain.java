package io.tapdata.indices;

import com.tapdata.entity.*;
//import io.tapdata.indices.mock.ConnectionsMock;
//import io.tapdata.indices.mock.RelateDatabaseTableMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 索引逻辑测试
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午3:07
 * </pre>
 */
public class TestMain {
	private AtomicInteger endCounts = new AtomicInteger();
	private AtomicInteger indexCounts = new AtomicInteger();
	private List<RelateDataBaseTable> tables = new ArrayList<>();
	private List<Mapping> mappings = new ArrayList<>();
	private Consumer<MessageEntity> messageEntityConsumer = messageEntity -> {
		if (OperationType.CREATE_INDEX.getOp().equalsIgnoreCase(messageEntity.getOp())) {
			indexCounts.addAndGet(1);
			Map<String, Object> after = messageEntity.getAfter();
			Assert.assertNotNull("消息没有设置 'after' 属性", after);
			Assert.assertNotNull("消息没有设置 'after.schema' 属性", after.get("schema"));
			Assert.assertNotNull("消息没有设置 'after.tableIndex' 属性", after.get("tableIndex"));

		} else if (OperationType.END_DDL.getOp().equalsIgnoreCase(messageEntity.getOp())) {
			endCounts.addAndGet(1);
		}
	};

	@Before
	public void settings() {
//    tables.add(RelateDatabaseTableMock.mock("TEST_TABLE_NAME"));
		for (RelateDataBaseTable table : tables) {
			mappings.add(new Mapping(table.getTable_name(), table.getTable_name(), null));
		}

	}

	@Test
	public void testInstance() {
		// 检测：
		// 已实现的，能否正常获取
		// 未实现的，进行提示

		Set<DatabaseTypeEnum> types = new HashSet<>(Arrays.asList(
				DatabaseTypeEnum.MONGODB,
				DatabaseTypeEnum.MARIADB,
				DatabaseTypeEnum.MYSQL,
				DatabaseTypeEnum.MYSQL_PXC,
				DatabaseTypeEnum.MSSQL,
				DatabaseTypeEnum.POSTGRESQL,
				DatabaseTypeEnum.ORACLE
		));
		for (DatabaseTypeEnum databaseType : DatabaseTypeEnum.values()) {
			try {
				IIndices<Object> ins = IndicesUtil.getInstance(databaseType);
				Assert.assertTrue(String.format("索引实例，已实现未同步到单元测试 %s", databaseType.getName()), types.contains(databaseType));
			} catch (Exception e) {
				Assert.assertFalse(String.format("索引实例，初始化失败：%s", databaseType.getName()), types.contains(databaseType));
				System.out.println("未实现的索引实例：" + databaseType.getName());
			}
		}
	}

//  @Test
//  public void testGenerateMessageEntity() {
//    // 检测：
//    // 生成的消息，是否包含：after.schema, after.tableIndex 数据
//    // 生成结束消息控制，是否可用
//    // 同步开关，是否可用
//
//    Connections connections = ConnectionsMock.mock(DatabaseTypeEnum.MYSQL);
//
//    // 开启同步、关闭结束消息
//    IndicesUtil.generateCreateMessageEntity(true, connections, tables, mappings, messageEntityConsumer, false, o -> true);
//    Assert.assertNotEquals("没生成消息", 0, indexCounts.intValue());
//    Assert.assertEquals("不需要生成结束消息", 0, endCounts.intValue());
//
//    // 关闭同步、开启结束消息
//    endCounts.set(0);
//    indexCounts.set(0);
//    IndicesUtil.generateCreateMessageEntity(false, connections, tables, mappings, messageEntityConsumer, true, o -> true);
////		Assert.assertEquals("关闭同步还生成消息", 0, indexCounts.intValue()); // todo: 索引配置启用时解开注释
//    Assert.assertNotEquals("没有生成结束消息", 0, endCounts.intValue());
//  }
}
