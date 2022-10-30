package com.tapdata.tm.sdk.available;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.web.client.RestClientException;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class CloudRestTemplate extends TmAvailableRestTemplate {

	private final ObjectMapper objectMapper;

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
