package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewFinishReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewReadOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-11-05 15:17
 **/
@DisplayName("Class PreviewNormalReadTasklet Test")
class PreviewNormalReadTaskletTest {

	private PreviewNormalReadTasklet previewNormalReadTasklet;

	@BeforeEach
	void setUp() {
		previewNormalReadTasklet = new PreviewNormalReadTasklet();
	}

	@Nested
	@DisplayName("Method execute test")
	class executeTest {

		private TaskDto taskDto;
		private PreviewReadOperationQueue previewReadOperationQueue;

		@BeforeEach
		void setUp() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview2.json");
			previewReadOperationQueue = new PreviewReadOperationQueue(100);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			String nodeId1 = "ba059587-63fd-4ace-8a24-29a5f42c75e7";
			CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> assertDoesNotThrow(() -> previewNormalReadTasklet.execute(taskDto, previewReadOperationQueue)));
			PreviewOperation previewOperation1 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation1);
			assertInstanceOf(PreviewReadOperation.class, previewOperation1);
			PreviewReadOperation previewReadOperation1 = (PreviewReadOperation) previewOperation1;
			assertEquals(nodeId1, previewReadOperation1.getSourceNodeId());
			PreviewOperation previewOperation2 = assertDoesNotThrow(() -> previewReadOperationQueue.poll(nodeId1, 10L, TimeUnit.SECONDS));
			assertNotNull(previewOperation2);
			assertInstanceOf(PreviewFinishReadOperation.class, previewOperation2);
			assertTrue(((PreviewFinishReadOperation) previewOperation2).isLast());
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
	}
}