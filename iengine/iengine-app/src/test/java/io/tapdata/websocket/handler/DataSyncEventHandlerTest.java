package io.tapdata.websocket.handler;

import com.tapdata.entity.ResponseBody;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.websocket.WebSocketEventResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/8 18:41 Create
 */
public class DataSyncEventHandlerTest {

	@Nested
	class HandleTest {

		DataSyncEventHandler dataSyncEventHandler;
		ResponseBody responseBody;
		Map<String, Object> event;

		@BeforeEach
		void before() {
			dataSyncEventHandler = new DataSyncEventHandler();
			responseBody = new ResponseBody();
			event = new HashMap<>();
		}

		@Test
		void testHandleTmUnavailableException() {
			try (
				MockedStatic<OpType> opTypeMockedStatic = Mockito.mockStatic(OpType.class);
				MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = Mockito.mockStatic(TmUnavailableException.class)
			) {
				tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.isInstance(Mockito.any())).thenReturn(true);
				opTypeMockedStatic.when(() -> OpType.fromOp(Mockito.anyString())).thenThrow(new RuntimeException("xxx"));

				Object handle = dataSyncEventHandler.handle(event);

				Assertions.assertTrue(handle instanceof WebSocketEventResult);
				WebSocketEventResult result = (WebSocketEventResult) handle;

				Assertions.assertTrue(result.getError().startsWith("Handle task websocket event failed because TM unavailable, event"));
			}
		}

		@Test
		void testHandleTmAvailableException() {
			String errorMsg = "tm-available-exception";
			try (
				MockedStatic<OpType> opTypeMockedStatic = Mockito.mockStatic(OpType.class);
				MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = Mockito.mockStatic(TmUnavailableException.class)
			) {
				tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.isInstance(Mockito.any())).thenReturn(false);
				opTypeMockedStatic.when(() -> OpType.fromOp(Mockito.anyString())).thenThrow(new RuntimeException(errorMsg));

				Object handle = dataSyncEventHandler.handle(event);

				Assertions.assertTrue(handle instanceof WebSocketEventResult);
				WebSocketEventResult result = (WebSocketEventResult) handle;

				Assertions.assertTrue(result.getError().startsWith(errorMsg));
			}
		}

	}
}
