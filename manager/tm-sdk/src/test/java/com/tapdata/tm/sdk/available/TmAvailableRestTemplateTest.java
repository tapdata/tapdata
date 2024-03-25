package com.tapdata.tm.sdk.available;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/11 19:29 Create
 */
class TmAvailableRestTemplateTest {

	@Test
	void testDoExecuteException() {
		try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = Mockito.mockStatic(TmStatusService.class, Mockito.CALLS_REAL_METHODS)) {
			tmStatusServiceMockedStatic.when(TmStatusService::isEnable).thenReturn(true);

			ClientHttpRequestFactory clientHttpRequestFactory = Mockito.mock(ClientHttpRequestFactory.class);
			TmAvailableRestTemplate restTemplate = new TmAvailableRestTemplate(clientHttpRequestFactory) {
				int i = 0;

				@Override
				protected ClientHttpRequest createRequest(URI url, HttpMethod method) throws IOException {
					if (i++ > 1) {
						throw new IOException("Read timed out");
					}
					throw new IOException("other error");
				}
			};

			Object availableNotTimeout = null;
			Object unavailableNotTimeout = null;
			Object availableTimeout = null;

			URI url = URI.create("http://localhost:8080");
			Assertions.assertTrue(availableNotTimeout == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
			Assertions.assertTrue(TmStatusService.isNotAvailable());
			Assertions.assertTrue(unavailableNotTimeout == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));

			TmStatusService.setAvailable();
			url = URI.create("http://localhost:8080?param=xxx");
			Assertions.assertTrue(availableTimeout == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
		}
	}

	@Test
	void testDoExecute() throws Exception {
		try (MockedStatic<TmStatusService> tmStatusServiceMockedStatic = Mockito.mockStatic(TmStatusService.class, Mockito.CALLS_REAL_METHODS)) {
			tmStatusServiceMockedStatic.when(TmStatusService::isEnable).thenReturn(true);

			ClientHttpRequestFactory clientHttpRequestFactory = Mockito.mock(ClientHttpRequestFactory.class);
			ClientHttpRequest clientHttpRequest = Mockito.mock(ClientHttpRequest.class);
			Mockito.when(clientHttpRequest.execute()).thenReturn(new ClientHttpResponse() {
				int i = 0;

				@Override
				public HttpStatus getStatusCode() throws IOException {
					if (i++ > 1) {
						return HttpStatus.OK;
					}
					return HttpStatus.SERVICE_UNAVAILABLE;
				}

				@Override
				public int getRawStatusCode() throws IOException {
					return 0;
				}

				@Override
				public String getStatusText() throws IOException {
					return null;
				}

				@Override
				public void close() {

				}

				@Override
				public InputStream getBody() throws IOException {
					return null;
				}

				@Override
				public HttpHeaders getHeaders() {
					return null;
				}
			});
			TmAvailableRestTemplate restTemplate = new TmAvailableRestTemplate(clientHttpRequestFactory) {
				@Override
				protected ClientHttpRequest createRequest(URI url, HttpMethod method) throws IOException {
					return clientHttpRequest;
				}
			};

			Object from503available = null;
			Object from503unavailable = null;
			Object from200unavailable = null;
			Object from200available = null;

			URI url = URI.create("http://localhost:8080");
			TmStatusService.setAvailable();
			Assertions.assertTrue(from503available == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
			Assertions.assertTrue(TmStatusService.isNotAvailable());
			Assertions.assertTrue(from503unavailable == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
			Assertions.assertTrue(TmStatusService.isNotAvailable());

			Assertions.assertTrue(from200unavailable == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
			Assertions.assertTrue(TmStatusService.isAvailable());
			Assertions.assertTrue(from200available == restTemplate.doExecute(url, HttpMethod.POST, null, response -> null));
			Assertions.assertTrue(TmStatusService.isAvailable());
		}
	}
}
