package com.tapdata.constant;

import com.google.common.collect.ImmutableMap;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2020-08-26 19:21
 **/
public class MessageUtilTest {

	@Test
	public void splitByDeleteOperationTest() {
		List<MessageEntity> messageEntities = new ArrayList<>();

		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_UPDATE, new HashMap<>(), "test"));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, new HashMap<>(), "test"));

//		System.out.println(String.format("Message entities size: %s", messageEntities.size()));

		List<List<MessageEntity>> lists = MessageUtil.splitMessages(messageEntities, true, false, "_");

		int count = 0;
		for (int i = 0; i < lists.size(); i++) {
//			System.out.println(String.format("=== %s ===", i));
			List<MessageEntity> list = lists.get(i);
			for (MessageEntity messageEntity : list) {
//				System.out.println(String.format("  %s", messageEntity.getOp()));
				count++;
			}
		}
		Assert.assertEquals("Split result size not match", 6, lists.size());
		Assert.assertEquals("Message entities count not match", messageEntities.size(), count);
	}

	@Test
	public void splitByJoinKeyTest() {
		List<MessageEntity> messageEntities = new ArrayList<>();


		Mapping mapping = new Mapping(
				"CAR_CLAIM",
				"CAR_CLAIM_TEST",
				Arrays.asList(
						new HashMap<String, String>() {{
							put("POLICY_ID", "POLICY_ID");
							put("CLAIM_DATE", "CLAIM_DATE");
						}}
				)
		);
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT,
						ImmutableMap.<String, Object>builder()
								.put("POLICY_ID", "PC_000000003")
								.put("CLAIM_DATE", new Date(1509235200000L))
								.put("CLAIM_REASON", "HAIL")
								.put("SETTLED_AMOUNT", 2024.00)
								.build(),
						"CAR_CLAIM",
						mapping
				)
		);

		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000004")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235300000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000002")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_UPDATE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000004")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_UPDATE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000002")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000002")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_UPDATE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_DELETE, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));
		messageEntities.add(new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, ImmutableMap.<String, Object>builder()
				.put("POLICY_ID", "PC_000000003")
				.put("CLAIM_DATE", new Date(1509235200000L))
				.put("CLAIM_REASON", "HAIL")
				.put("SETTLED_AMOUNT", 2024.00)
				.build(), "CAR_CLAIM",
				mapping
		));


//		System.out.println(String.format("Message entities size: %s", messageEntities.size()));

		List<List<MessageEntity>> lists = MessageUtil.splitMessages(messageEntities, true, true, "_");

		int count = 0;
		for (int i = 0; i < lists.size(); i++) {
//			System.out.println(String.format("=== %s ===", i));
			List<MessageEntity> list = lists.get(i);
			for (MessageEntity messageEntity : list) {
//				System.out.println(String.format("  %s", messageEntity.getOp()));
				count++;
			}
		}
		Assert.assertEquals("Split result size not match", 9, lists.size());
		Assert.assertEquals("Message entities count not match", messageEntities.size(), count);
	}

	@Test
	public void splitGroupByDmlOpTest() {
		List<MessageEntity> msgs = new ArrayList<>();
		msgs.add(new MessageEntity(OperationType.INSERT.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.INSERT.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.INSERT.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.UPDATE.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.INSERT.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.DELETE.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.DELETE.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.INSERT.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.DDL.getOp(), new HashMap<>(), "test"));
		msgs.add(new MessageEntity(OperationType.DDL.getOp(), new HashMap<>(), "test"));

		List<String> actualResult = new ArrayList<>();
		AtomicInteger count = new AtomicInteger(0);
		MessageUtil.splitGroupByDmlOp(msgs,
				im -> {
					actualResult.add("insert-" + im.size());
					count.set(count.get() + im.size());
				},
				um -> {
					actualResult.add(("update-" + um.size()));
					count.set(count.get() + um.size());
				},
				dm -> {
					actualResult.add("delete-" + dm.size());
					count.set(count.get() + dm.size());
				},
				nm -> {
					actualResult.add("notDml-" + nm.size());
					count.set(count.get() + nm.size());
				}, null, null
		);
		List<String> expectResult = new ArrayList<>();
		expectResult.add("insert-3");
		expectResult.add("update-1");
		expectResult.add("insert-1");
		expectResult.add("delete-2");
		expectResult.add("insert-1");
		expectResult.add("notDml-2");

		Assert.assertEquals(String.join(",", expectResult), String.join(",", actualResult));
		Assert.assertEquals(msgs.size(), count.get());
	}
}
