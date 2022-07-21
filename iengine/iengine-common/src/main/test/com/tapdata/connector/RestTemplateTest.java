package com.tapdata.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.reflect.TypeToken;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.*;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.*;

/**
 * @author lg
 * Create by lg on 4/28/20 2:54 PM
 */
public class RestTemplateTest {

	@Before
	public void before() {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig rootLoggerConfig = config.getRootLogger();
		rootLoggerConfig.setLevel(Level.DEBUG);
		ctx.updateLoggers();
	}

	private ClientHttpRequestFactory getRequestFactory() {
		/*SimpleClientHttpRequestFactory simpleClientHttpRequestFactory = new SimpleClientHttpRequestFactory();
		simpleClientHttpRequestFactory.setConnectTimeout(60000);
		simpleClientHttpRequestFactory.setReadTimeout(60000);
		return simpleClientHttpRequestFactory;*/

		/*DefaultHttpClient client = new ContentEncodingHttpClient();
		client.addRequestInterceptor((request, context) -> {
			if (!request.containsHeader("Accept-Encoding")) {
				request.addHeader("Accept-Encoding", "gzip");
			}
		});
		client.addResponseInterceptor((response, context) -> {
			org.apache.http.HttpEntity entity = response.getEntity();
			if (entity != null) {
				Header ceheader = entity.getContentEncoding();
				if (ceheader != null) {
					HeaderElement[] codecs = ceheader.getElements();
					for (int i = 0; i < codecs.length; i++) {
						if (codecs[i].getName().equalsIgnoreCase("gzip")) {
							response.setEntity(
									new GzipDecompressingEntity(response.getEntity()));
							return;
						}
					}
				}
			}
		});*/
		int threshold = 1024;
		HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(
				//client
				HttpClientBuilder.create()
						.setDefaultRequestConfig(
								RequestConfig.custom()
										.setContentCompressionEnabled(true)
										.setDecompressionEnabled(true)
										.build())
						.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
							if (request instanceof HttpEntityEnclosingRequest) {
								HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;

								if (enclosingRequest.getEntity() instanceof ByteArrayEntity) {
									ByteArrayEntity byteArrayEntity = (ByteArrayEntity) enclosingRequest.getEntity();
									long contentLength = byteArrayEntity.getContentLength();
									if (contentLength > threshold) {
										request.addHeader(org.apache.http.HttpHeaders.CONTENT_ENCODING, "gzip");
										enclosingRequest.setEntity(new GzipCompressingEntity(enclosingRequest.getEntity()));
									}
								}
							}
						})
						/*.setHttpProcessor(new HttpProcessor() {
						  @Override
						  public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {

							request.addHeader(org.apache.http.HttpHeaders.ACCEPT_ENCODING, "gzip,deflate");

							if( request instanceof HttpEntityEnclosingRequest){
							  HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;

							  if( enclosingRequest.getEntity() instanceof ByteArrayEntity){
								ByteArrayEntity byteArrayEntity = (ByteArrayEntity) enclosingRequest.getEntity();
								long contentLength = byteArrayEntity.getContentLength();
								if( contentLength > threshold){
								  request.addHeader(org.apache.http.HttpHeaders.CONTENT_ENCODING, "gzip");
								  enclosingRequest.setEntity(new GzipCompressingEntity(enclosingRequest.getEntity()));
								}
							  }

							}
						  }

						  @Override
						  public void process(HttpResponse response, HttpContext context) throws HttpException, IOException {
							Header[] contentEncodingHeader = response.getHeaders(org.apache.http.HttpHeaders.CONTENT_ENCODING);
							String contentEncoding = contentEncodingHeader != null && contentEncodingHeader.length > 0 ? contentEncodingHeader[0].getValue() : null;
							if( "gzip".equalsIgnoreCase(contentEncoding)) {
							  response.setEntity(new GzipDecompressingEntity(response.getEntity()));
							} else if( "deflate".equalsIgnoreCase(contentEncoding)) {
							  response.setEntity(new DeflateDecompressingEntity(response.getEntity()));
							}
						  }
						})*/
						.build()
		);
		requestFactory.setConnectTimeout(60000);
		requestFactory.setReadTimeout(60000);
		return requestFactory;

		/*OkHttp3ClientHttpRequestFactory requestFactory = new OkHttp3ClientHttpRequestFactory();
		requestFactory.setConnectTimeout(60000);
		requestFactory.setReadTimeout(60000);
		return requestFactory;*/
	}

	@Test
	public void testRequestEnableCompress() {

		RestTemplate restTemplate = new RestTemplate(getRequestFactory());
		List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
		messageConverters.add(new MappingJackson2HttpMessageConverter());
		restTemplate.setMessageConverters(messageConverters);
		// restTemplate.getInterceptors().add(new CompressingClientHttpRequestInterceptor());

		HttpHeaders requestHeaders = new HttpHeaders();
		requestHeaders.add(HttpHeaders.COOKIE, "isAdmin=1;user_id=5e0efc121148955cf1ad96d1");

		HttpEntity<String> httpEntity = new HttpEntity<>(requestHeaders);
		URI uri = URI.create("http://127.0.0.1:3030/api/Connections?access_token=0n0npeB9tbYVh2qCEJo9KjvA28nOFoYQLRPodGs3xXbuW4Aus28fgUmthMlX4ldt");
		ResponseEntity<List> responseEntity = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, List.class);

		System.out.println(responseEntity.getStatusCode());
		//System.out.println(responseEntity.getBody());

		System.out.println();

		uri = URI.create("http://127.0.0.1:3030/api/Logs/?access_token=0n0npeB9tbYVh2qCEJo9KjvA28nOFoYQLRPodGs3xXbuW4Aus28fgUmthMlX4ldt");
		Map<String, Object> reqBody = new HashMap<>();
		reqBody.put("loggerName", "test");
		reqBody.put("level", "debug");
		reqBody.put("message", "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");
		// reqBody.put("message", "request body size lt 1024, don't compress");
		HttpEntity<Map> httpEntity1 = new HttpEntity<>(reqBody, requestHeaders);
		ResponseEntity<Map> responseEntity1 = restTemplate.exchange(uri, HttpMethod.POST, httpEntity1, Map.class);

		System.out.println(responseEntity1.getStatusCode());
		System.out.println(responseEntity1.getBody());
	}

	@Test
	public void testType() {
		Type type = new TypeToken<RestTemplateTest>() {
		}.getType();
		System.out.println(type.getTypeName());
	}

}
