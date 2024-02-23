package com.tapdata.cache.scripts;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashSet;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-02-06 11:42
 **/
@DisplayName("ScriptCacheServiceTest Class Test")
public class ScriptCacheServiceTest {

	@Nested
	@DisplayName("constructor method test")
	class constructorTest {
		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
			TaskDto taskDto = new TaskDto();
			ObjectId taskId = new ObjectId();
			taskDto.setId(taskId);
			taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
			TableNode tableNode = new TableNode();
			tableNode.setId("1");
			ICacheService cacheService = mock(ICacheService.class);
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
			when(dataProcessorContext.getCacheService()).thenReturn(cacheService);
			ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, dataProcessorContext);
			assertEquals(taskId.toHexString(), ReflectionTestUtils.getField(scriptCacheService, "taskId"));
			assertEquals("1", ReflectionTestUtils.getField(scriptCacheService, "nodeId"));
			assertFalse((boolean) ReflectionTestUtils.getField(scriptCacheService, "normalTask"));
			assertEquals(clientMongoOperator, ReflectionTestUtils.getField(scriptCacheService, "clientMongoOperator"));
			assertEquals(cacheService, ReflectionTestUtils.getField(scriptCacheService, "supperCacheService"));
		}
	}

	@Nested
	class setTaskUsedInfoTest {
		@Test
		@DisplayName("when normal task")
		void isNormalTask() {
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
			TaskDto taskDto = new TaskDto();
			ObjectId taskId = new ObjectId();
			when(clientMongoOperator.update(eq(Query.query(Criteria.where("_id").is(taskId.toHexString()))),
					any(Update.class), eq(ConnectorConstant.TASK_COLLECTION))).thenReturn(null);
			taskDto.setId(taskId);
			taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TableNode tableNode = new TableNode();
			tableNode.setId("1");
			ICacheService cacheService = mock(ICacheService.class);
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
			when(dataProcessorContext.getCacheService()).thenReturn(cacheService);
			ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, dataProcessorContext);

			scriptCacheService.setTaskUsedInfo("test");
			verify(clientMongoOperator, times(1)).update(eq(Query.query(Criteria.where("_id").is(taskId.toHexString()))),
					any(Update.class), eq(ConnectorConstant.TASK_COLLECTION));
			Object useInfo = ReflectionTestUtils.getField(scriptCacheService, "useInfo");
			assertInstanceOf(HashSet.class, useInfo);
			assertTrue(((HashSet) useInfo).contains("test"));
		}

		@Test
		@DisplayName("test run or deduce schema task")
		void isNotNormalTask() {
			ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
			TaskDto taskDto = new TaskDto();
			ObjectId taskId = new ObjectId();
			when(clientMongoOperator.update(eq(Query.query(Criteria.where("_id").is(taskId.toHexString()))),
					any(Update.class), eq(ConnectorConstant.TASK_COLLECTION))).thenReturn(null);
			taskDto.setId(taskId);
			taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
			TableNode tableNode = new TableNode();
			tableNode.setId("1");
			ICacheService cacheService = mock(ICacheService.class);
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
			when(dataProcessorContext.getCacheService()).thenReturn(cacheService);
			ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, dataProcessorContext);

			scriptCacheService.setTaskUsedInfo("test");
			verify(clientMongoOperator, never()).update(eq(Query.query(Criteria.where("_id").is(taskId.toHexString()))),
					any(Update.class), eq(ConnectorConstant.TASK_COLLECTION));
			Object useInfo = ReflectionTestUtils.getField(scriptCacheService, "useInfo");
			assertInstanceOf(HashSet.class, useInfo);
			assertTrue(((HashSet) useInfo).isEmpty());
		}
	}
}
