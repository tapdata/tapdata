package io.tapdata.exception;

import com.tapdata.entity.ResponseBody;
import com.tapdata.tm.sdk.available.TmStatusService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/11 19:15 Create
 */
class TmUnavailableExceptionTest {

	@Test
	void testIsInstanceTrue() {
		ResponseBody responseBody = Mockito.mock(ResponseBody.class);
		Assertions.assertFalse(TmUnavailableException.isInstance(new TmUnavailableException(new RuntimeException("test"), "test-url", "post", null, responseBody)));
	}

	@Test
	void testIsInstanceEnableTrue() {
		try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = Mockito.mockStatic(TmStatusService.class)) {
			tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(false);

			ResponseBody responseBody = Mockito.mock(ResponseBody.class);
			Assertions.assertTrue(TmUnavailableException.isInstance(new TmUnavailableException("test-url", "post", null, responseBody)));
			Assertions.assertFalse(TmUnavailableException.notInstance(new RuntimeException("wrapper exception", new TmUnavailableException("test-url", "post", null, responseBody))));
		}
	}
	@Test
	void testIsInstanceEnableFalse() {
		try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = Mockito.mockStatic(TmStatusService.class)) {
			tmStatusServiceMockedStatic.when(TmStatusService::isNotEnable).thenReturn(false);
			Assertions.assertTrue(TmUnavailableException.notInstance(new RuntimeException("Not TmUnavailableException")));
		}
	}

}
