package com.tapdata.tm.sdk.available;

import com.tapdata.tm.sdk.util.JacksonUtil;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.Assert;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TmAvailableRestTemplate extends RestTemplate {

  public TmAvailableRestTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
  }

	@Override
	protected <T> T doExecute(URI url, HttpMethod method, RequestCallback requestCallback, ResponseExtractor<T> responseExtractor) throws RestClientException {
		Assert.notNull(url, "'url' must not be null");
		Assert.notNull(method, "'method' must not be null");
		Assert.notNull(responseExtractor, "'responseExtractor' must not be null");
		long start = 0;
		try {
			ClientHttpRequest request = createRequest(url, method);
			if (requestCallback != null) {
				requestCallback.doWithRequest(request);
			}
			start = System.currentTimeMillis();

			try (ClientHttpResponse response = request.execute()) {
				//long ttl = System.currentTimeMillis() - start;
				HttpStatusCode statusCode = response.getStatusCode();
				if (statusCode.is5xxServerError()) {
					if (TmStatusService.isAvailable()) {
						logger.warn("TM unavailable, status code is " + response.getStatusText());
						TmStatusService.setNotAvailable();
					}
				} else if (!TmStatusService.isAvailable()) {
					TmStatusService.setAvailable();
					logger.warn("TM available...");
				}
				handleResponse(url, method, response);
				return responseExtractor.extractData(response);
			}
		} catch (IOException ex) {
			long ttl = System.currentTimeMillis() - start;
			if (TmStatusService.isAvailable()) {
				String resource = url.toString();
				String query = url.getRawQuery();
				resource = (query != null ? resource.substring(0, resource.indexOf('?')) : resource);
				TmStatusService.setNotAvailable();
				if ("Read timed out".equalsIgnoreCase(ex.getMessage())) {
					logger.warn("Tm disconnect, I/O error on " + method.name() + " request for \"" + resource + "\", ttl: " + ttl + "ms ,: " + ex.getMessage());
				} else {
					logger.warn("Tm disconnect, I/O error on " + method.name() + " request for \"" + resource + "\", ttl: " + ttl + "ms ,: " + ex.getMessage(), ex);
				}
			}
			try {
				return responseExtractor.extractData(getDefaultResponse());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}


  private AbstractClientHttpResponse getDefaultResponse() {
    return new AbstractClientHttpResponse();
  }
  class AbstractClientHttpResponse implements ClientHttpResponse {


	  @Override
	  public HttpStatusCode getStatusCode() throws IOException {
		  return HttpStatus.OK;
	  }

	  @Override
	  public String getStatusText() throws IOException {
		  return "ok";
	  }

	  @Override
	  public void close() {

	  }

	  @Override
	  public InputStream getBody() throws IOException {
		  Map<String, Object> responseBody = new HashMap<String, Object>() {{
			  put("code", "503");
			  put("msg", "503 Service Unavailable");
		  }};
		  String obj2Json = JacksonUtil.toJson(responseBody);
		  byte[] bytes = obj2Json.getBytes(StandardCharsets.UTF_8);
		  return new ByteArrayInputStream(bytes);
	  }

	  @Override
	  public HttpHeaders getHeaders() {
		  HttpHeaders httpHeaders = new HttpHeaders();
		  httpHeaders.setContentType(MediaType.APPLICATION_JSON);
		  return httpHeaders;
	  }
  }

}
