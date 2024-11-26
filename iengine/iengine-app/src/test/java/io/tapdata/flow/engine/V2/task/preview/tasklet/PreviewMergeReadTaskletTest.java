package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeReadData;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewFinishReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-11-05 11:55
 **/
@DisplayName("Class PreviewMergeReadTasklet Test")
class PreviewMergeReadTaskletTest {

	private PreviewMergeReadTasklet previewMergeReadTasklet;
	private TaskDto taskDto;

	@BeforeEach
	void setUp() {
		previewMergeReadTasklet = new PreviewMergeReadTasklet();
	}

	@Nested
	@DisplayName("Method execute test")
	class executeTest {

		private PreviewReadOperationQueue previewReadOperationQueue;

		@BeforeEach
		void setUp() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview1.json");
			previewReadOperationQueue = new PreviewReadOperationQueue(100);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			String nodeId1 = "e3a5ef29-90d2-459e-90c5-d27e813e8d0c";
			String nodeId2 = "7c2e681f-a875-4bb2-b0b7-5ae5a9ac19d3";
			CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> assertDoesNotThrow(() -> previewMergeReadTasklet.execute(taskDto, previewReadOperationQueue)));
			PreviewOperation previewOperation1 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation1);
			assertInstanceOf(PreviewMergeReadOperation.class, previewOperation1);
			PreviewMergeReadOperation previewMergeReadOperation1 = (PreviewMergeReadOperation) previewOperation1;
			assertEquals(nodeId1, previewMergeReadOperation1.getSourceNodeId());
			assertNotNull(previewMergeReadOperation1.getMergeTableLoopProperty());
			assertEquals(1, previewMergeReadOperation1.getMergeTableLoopProperty().getLevel());
			assertNotNull(previewMergeReadOperation1.getMergeTableLoopProperty().getMergeTableProperties());
			Document replyData1 = new Document("CUSTOMER_ID", "1").append("FIRST_NAME", "test");
			MergeReadData mergeReadData1 = new MergeReadData(Arrays.asList(replyData1));
			assertDoesNotThrow(() -> previewMergeReadOperation1.replyData(mergeReadData1));
			previewMergeReadOperation1.getMergeNodeReceived().countDown();
			PreviewOperation previewOperation2 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId2, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation2);
			assertInstanceOf(PreviewMergeReadOperation.class, previewOperation2);
			PreviewMergeReadOperation previewMergeReadOperation2 = (PreviewMergeReadOperation) previewOperation2;
			assertEquals(nodeId2, previewMergeReadOperation2.getSourceNodeId());
			assertNotNull(previewMergeReadOperation2.getMergeTableLoopProperty());
			assertEquals(2, previewMergeReadOperation2.getMergeTableLoopProperty().getLevel());
			Document replyData2 = new Document("POLICY_ID", "1").append("CUSTOMER_ID", "1");
			MergeReadData mergeReadData2 = new MergeReadData(Arrays.asList(replyData2));
			assertDoesNotThrow(() -> previewMergeReadOperation2.replyData(mergeReadData2));
			previewMergeReadOperation2.getMergeNodeReceived().countDown();
			PreviewOperation previewOperation3 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation3);
			assertInstanceOf(PreviewFinishReadOperation.class, previewOperation3);
			assertFalse(((PreviewFinishReadOperation) previewOperation3).isLast());
			PreviewOperation previewOperation4 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId2, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation4);
			assertInstanceOf(PreviewFinishReadOperation.class, previewOperation4);
			assertTrue(((PreviewFinishReadOperation) previewOperation4).isLast());
			long currentTimeMillis = System.currentTimeMillis();
			while (true) {
				if (completableFuture.isDone()) {
					break;
				}
				if (System.currentTimeMillis() - currentTimeMillis > TimeUnit.SECONDS.toMillis(10L)) {
					try {
						assertTrue(completableFuture.isDone());
					} finally {
						if (!completableFuture.isDone()) {
							completableFuture.cancel(true);
						}
					}
				}
			}
		}

		@Test
		@SneakyThrows
		@DisplayName("test main process when sleep throw an exception")
		void test2() {
			String nodeId1 = "e3a5ef29-90d2-459e-90c5-d27e813e8d0c";
			previewMergeReadTasklet = spy(previewMergeReadTasklet);
			doThrow(new InterruptedException()).when(previewMergeReadTasklet).sleep();
			PreviewOperation previewOperation1 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation1);
			assertInstanceOf(PreviewMergeReadOperation.class, previewOperation1);
			PreviewMergeReadOperation previewMergeReadOperation1 = (PreviewMergeReadOperation) previewOperation1;
			assertEquals(nodeId1, previewMergeReadOperation1.getSourceNodeId());
			assertNotNull(previewMergeReadOperation1.getMergeTableLoopProperty());
			assertEquals(1, previewMergeReadOperation1.getMergeTableLoopProperty().getLevel());
			assertNotNull(previewMergeReadOperation1.getMergeTableLoopProperty().getMergeTableProperties());
			Document replyData1 = new Document("CUSTOMER_ID", "1").append("FIRST_NAME", "test");
			MergeReadData mergeReadData1 = new MergeReadData(Arrays.asList(replyData1));
			assertDoesNotThrow(() -> previewMergeReadOperation1.replyData(mergeReadData1));
			previewMergeReadOperation1.getMergeNodeReceived().countDown();
			PreviewOperation previewOperation3 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNull(previewOperation3);
		}
	}
}