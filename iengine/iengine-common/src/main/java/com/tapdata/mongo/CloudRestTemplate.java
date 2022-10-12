package com.tapdata.mongo;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.*;
import org.springframework.http.client.AbstractClientHttpResponse;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class CloudRestTemplate extends RestTemplate {

	private ObjectMapper objectMapper;

	public CloudRestTemplate(ClientHttpRequestFactory requestFactory) {
		super(requestFactory);
		this.objectMapper = this.getMessageConverters().stream().filter(c -> c instanceof MappingJackson2HttpMessageConverter).findFirst()
				.map(e -> (MappingJackson2HttpMessageConverter) e).orElse(new MappingJackson2HttpMessageConverter()).getObjectMapper();
		DefaultUriBuilderFactory defaultUriBuilderFactory = new DefaultUriBuilderFactory();
		defaultUriBuilderFactory.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.NONE);
		this.setUriTemplateHandler(defaultUriBuilderFactory);
	}

	@Override
	public <T> ResponseEntity<T> postForEntity(String url, @Nullable Object request,
											   Class<T> responseType, Object... uriVariables) throws RestClientException {
		url = CloudSignUtil.getQueryStr(HttpMethod.POST.name(), url, body2Json(request));
		return super.postForEntity(url, request, responseType, uriVariables);
	}

	@Override
	public <T> ResponseEntity<T> postForEntity(URI url, @Nullable Object request, Class<T> responseType) throws RestClientException {
		return this.postForEntity(url.toString(), request, responseType);
	}

	@Override
	public void delete(String url, Object... uriVariables) throws RestClientException {
		String str = getUriTemplateHandler().expand(url, uriVariables).toString();
		super.delete(CloudSignUtil.getQueryStr(HttpMethod.DELETE.name(), str, ""));
	}

	@Override
	public <T> T postForObject(URI url, Object request, Class<T> responseType) throws RestClientException {
		String queryStr = CloudSignUtil.getQueryStr(HttpMethod.POST.name(), url.toString(), body2Json(request));
		url = URI.create(queryStr);
		return super.postForObject(url, request, responseType);
	}

	@Override
	public <T> ResponseEntity<T> exchange(String url, HttpMethod method,
										  @Nullable HttpEntity<?> requestEntity, Class<T> responseType, Object... uriVariables)
			throws RestClientException {
		url = CloudSignUtil.getQueryStr(method.name(), url, body2Json(requestEntity.getBody()));
		return super.exchange(url, method, requestEntity, responseType, uriVariables);
	}

	@Override
	public <T> ResponseEntity<T> exchange(URI url, HttpMethod method, HttpEntity<?> requestEntity, Class<T> responseType) throws RestClientException {
		String bodyStr = "";
		if (requestEntity != null) {
			bodyStr = body2Json(requestEntity.getBody());
		}
		String queryStr = CloudSignUtil.getQueryStr(method.name(), url.toString(), bodyStr);
		return super.exchange(queryStr, method, requestEntity, responseType);
	}

	@Override
	protected <T> T doExecute(URI url, HttpMethod method, RequestCallback requestCallback, ResponseExtractor<T> responseExtractor) throws RestClientException {
		Assert.notNull(url, "'url' must not be null");
		Assert.notNull(method, "'method' must not be null");
		ClientHttpResponse response = null;
		try {

			if (TmStatusService.isNotAllowReport()) {
				logger.warn("Tm not available, skip report, -> " + url);
				return responseExtractor.extractData(getDefaultResponse());
			}
			ClientHttpRequest request = createRequest(url, method);
			if (requestCallback != null) {
				requestCallback.doWithRequest(request);
			}
			response = request.execute();
			HttpStatus statusCode = response.getStatusCode();
			if (statusCode.is5xxServerError() && TmStatusService.isAvailable()) {
				logger.warn("Tm not available, status code is " + response.getStatusText());
				TmStatusService.setNotAvailable();
			} else if (!TmStatusService.isAvailable()) {
				TmStatusService.setAvailable();
				logger.warn("Tm available...");
			}
			handleResponse(url, method, response);
			if (responseExtractor != null) {
				return responseExtractor.extractData(response);
			}
			else {
				return null;
			}
		}
		catch (IOException ex) {
			if (TmStatusService.isAvailable()) {
				String resource = url.toString();
				String query = url.getRawQuery();
				resource = (query != null ? resource.substring(0, resource.indexOf('?')) : resource);
				TmStatusService.setNotAvailable();
				logger.warn("Tm disconnect, I/O error on " + method.name() + " request for \"" + resource + "\": " + ex.getMessage(), ex);
			}
			if (responseExtractor != null) {
				try {
					return responseExtractor.extractData(getDefaultResponse());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		finally {
			if (response != null) {
				response.close();
			}
		}
		return null;
	}

	@NotNull
	private static AbstractClientHttpResponse getDefaultResponse() {
		return new AbstractClientHttpResponse() {

			@Override
			public int getRawStatusCode() throws IOException {
				return 200;
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
				return null;
			}

			@Override
			public HttpHeaders getHeaders() {
				return null;
			}
		};
	}

	private String body2Json(Object body) {
		try {
			if (body == null) {
				return "";
			}
			JsonEncoding encoding = JsonEncoding.UTF8;
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			JsonGenerator generator = this.objectMapper.getFactory().createGenerator(byteArrayOutputStream, encoding);
			this.objectMapper.writer().writeValue(generator, body);

			return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			return "";
		}
	}


}
