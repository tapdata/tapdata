package io.tapdata.flow.engine.V2.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-24 18:20
 **/
class ErrorHandleExceptionTest {
	@Test
	@DisplayName("Main process test")
	void testGetMessage() {
		RuntimeException originEx = new RuntimeException("original exception");
		RuntimeException cause = new RuntimeException("error handle exception");
		ErrorHandleException errorHandleException = new ErrorHandleException(cause, originEx);
		String message = errorHandleException.getMessage();
		assertNotNull(message);
		assertTrue(message.contains(originEx.getMessage()));
	}

	@Test
	@DisplayName("GetMessage method when original exception is null")
	void testGetMessageWhenOriginalExceptionIsNull() {
		RuntimeException cause = new RuntimeException("error handle exception");
		ErrorHandleException errorHandleException = new ErrorHandleException(cause, null);
		String message = assertDoesNotThrow(errorHandleException::getMessage);
		assertNotNull(message);
	}
}
