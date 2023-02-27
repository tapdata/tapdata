package com.tapdata.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.BaseEntity;
import com.tapdata.entity.ResponseBody;
import com.tapdata.entity.TapLog;
import com.tapdata.interceptor.LoggingInterceptor;
import com.tapdata.tm.sdk.available.CloudRestTemplate;
import com.tapdata.tm.sdk.available.TmStatusService;
import com.tapdata.tm.sdk.interceptor.VersionHeaderInterceptor;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import io.tapdata.exception.ManagementException;
import io.tapdata.exception.RestAuthException;
import io.tapdata.exception.RestDoNotRetryException;
import io.tapdata.exception.RestException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RestTemplateOperator {

	private Logger logger = LogManager.getLogger(RestTemplateOperator.class);

	private RestTemplate restTemplate;

	private volatile String baseURL;

	private int retryTime;

	private List<String> baseURLs;

	private int size;

	private final static String NEW_INSTANCE_EXCEPTION = "Failed to new instance: %s";

	private Supplier<Long> getRetryTimeout;

	private long retryInterval = 500;

	private final AtomicLong logCount = new AtomicLong(0);

	private RestTemplateOperator() {
	}

	public RestTemplateOperator(List<String> baseURLs, int retryTime) {
		if (CloudSignUtil.isNeedSign()) {
			this.restTemplate = new CloudRestTemplate(getRequestFactory(60000, 60000, 60000));
		} else {
			this.restTemplate = new RestTemplate(getRequestFactory(60000, 60000, 60000));
		}
		List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();

		final ResourceHttpMessageConverter resourceHttpMessageConverter = new ResourceHttpMessageConverter();
		resourceHttpMessageConverter.setSupportedMediaTypes(
				ImmutableList.of(
						MediaType.APPLICATION_OCTET_STREAM
				)
		);
		messageConverters.add(new MappingJackson2HttpMessageConverter());
		messageConverters.add(resourceHttpMessageConverter);
		restTemplate.setMessageConverters(messageConverters);
		restTemplate.getInterceptors().add(new LoggingInterceptor());
		restTemplate.getInterceptors().add(new VersionHeaderInterceptor());

		this.retryTime = retryTime;

		this.baseURLs = baseURLs;
		this.baseURL = baseURLs.get(0);
		this.size = baseURLs.size();
	}

	public RestTemplateOperator(List<String> baseURLs, int retryTime, Supplier<Long> getRetryTimeout) {
		this(baseURLs, retryTime);
		this.getRetryTimeout = getRetryTimeout;
	}

	public RestTemplateOperator(List<String> baseURLs, int retryTime, Supplier<Long> getRetryTimeout, int connectTimeout, int readTimeout, int connectRequestTimeout) {
		if (CloudSignUtil.isNeedSign()) {
			this.restTemplate = new CloudRestTemplate(getRequestFactory(connectTimeout, readTimeout, connectRequestTimeout));
		} else {
			this.restTemplate = new RestTemplate(getRequestFactory(connectTimeout, readTimeout, connectRequestTimeout));
		}
		List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();

		final ResourceHttpMessageConverter resourceHttpMessageConverter = new ResourceHttpMessageConverter();
		resourceHttpMessageConverter.setSupportedMediaTypes(
				ImmutableList.of(
						MediaType.APPLICATION_OCTET_STREAM
				)
		);
		messageConverters.add(new MappingJackson2HttpMessageConverter());
		messageConverters.add(resourceHttpMessageConverter);

		restTemplate.setMessageConverters(messageConverters);
		restTemplate.getInterceptors().add(new LoggingInterceptor());
		restTemplate.getInterceptors().add(new VersionHeaderInterceptor());

		this.retryTime = retryTime;

		this.baseURLs = baseURLs;
		this.baseURL = baseURLs.get(0);
		this.size = baseURLs.size();
		this.getRetryTimeout = getRetryTimeout;
	}

	private ClientHttpRequestFactory getRequestFactory(int connectTimeout, int readTimeout, int connectRequestTimeout) {
		int threshold = 1024;

		PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
		poolingHttpClientConnectionManager.setMaxTotal(2000);
		poolingHttpClientConnectionManager.setDefaultMaxPerRoute(2000);

		CloseableHttpClient httpClient = HttpClientBuilder.create()
				.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
				.disableAutomaticRetries()
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
				.addInterceptorLast((HttpResponseInterceptor) (response, context) -> {
				})
				.setConnectionManager(poolingHttpClientConnectionManager)
				.build();

		HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
		requestFactory.setConnectTimeout(connectTimeout);
		requestFactory.setReadTimeout(readTimeout);
		requestFactory.setConnectionRequestTimeout(connectRequestTimeout);
		return requestFactory;
	}

	public boolean postOne(Object obj, String resource) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource);
					ResponseEntity<ResponseBody> responseEntity = restTemplate.postForEntity(url, obj, ResponseBody.class);
					responseBody = responseEntity.hasBody() ? responseEntity.getBody() : null;
					if (successResp(responseEntity)) {
						return true;
					} else {
						handleRequestFailed(url, HttpMethod.POST.name(), obj,
								responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
						);
						return true;
					}
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.POST.name(), obj, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return false;
		}
	}

	public Exception retryExceptionHandle(Exception e, String uri, String method, Object param, ResponseBody responseBody, int retryCount) {
		if (e instanceof HttpClientErrorException) {
			// If the parameter is incorrect, no retry will be performed
			if (404 == ((HttpClientErrorException) e).getRawStatusCode()) {
				throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), "not found url: " + uri), e);
			}
			if (405 == ((HttpClientErrorException) e).getRawStatusCode()) {
				throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), "Please upgrade engine"), e);
			}
		}
		if (retryCount <= 1) {
			logger.warn(
					"Request {} server failed {}, uri {}, method {}, param {}, response body {}, stack {}, will retry after {}.", baseURL, e.getMessage(), uri, param, responseBody, method, Log4jUtil.getStackString(e), retryInterval
			);
		}
		try {
			Thread.sleep(retryInterval);
		} catch (InterruptedException ignore) {
		}
		return e;
	}

	public boolean post(Object request, String resource, Map<String, Object> params) {
		return post(request, resource, params, null);
	}

	public boolean post(Object request, String resource, Map<String, Object> params, Predicate<?> stop) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();

		URI url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				if (null != stop && stop.test(null)) {
					break;
				}
				try {
					url = queryString(url(baseURL, resource), params);
					ResponseEntity<ResponseBody> responseEntity = restTemplate.postForEntity(url, request, ResponseBody.class);
					if (successResp(responseEntity)) {
						responseBody = responseEntity.getBody();
						Map body = getBody(responseBody, Map.class);
						if (body != null) {
							Object id = body.get("id");
							if (id != null) {
								if (request instanceof Map) {
									((Map) request).put("id", id);
								} else if (request instanceof BaseEntity) {
									((BaseEntity) request).setId(id.toString());
								}
							}
						}
						return true;
					} else {
						handleRequestFailed(url.toString(), HttpMethod.POST.name(), request,
								responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
						);
					}

					break;
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url != null ? url.toString() : null, HttpMethod.POST.name(), request, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}
		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return false;
		}
	}

	public void deleteById(String resource, Object... uriVariables) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		String url = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource);
					restTemplate.delete(url, uriVariables);
					return;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.DELETE.name(), uriVariables, null, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		}
	}

	public void delete(String resource, Map<String, Object> params) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		String url = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource);
					url = queryString(url, params).toString();
					restTemplate.delete(url, params);
					return;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.DELETE.name(), params, null, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		}
	}

	public void deleteAll(String resource, String operation, Map<String, Object> params) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();

		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource) + operation;
					HttpEntity httpEntity = new HttpEntity(params);
					ResponseEntity<ResponseBody> responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, ResponseBody.class);
					return;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.POST.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		}
	}

	public <T> T postOne(Object obj, String resource, Class<T> className) {
		return postOne(obj, resource, className, "");
//		String baseURL = this.baseURL;
//		int baseURLChangeTime = 0;
//
//		setRetryTime();
//		while (baseURLChangeTime < size) {
//			int retry = 0;
//			while (retry <= retryTime) {
//				try {
//					ResponseEntity<ResponseBody> responseEntity = restTemplate.postForEntity(url(baseURL, resource), obj, ResponseBody.class);
//
//					T result = null;
//					if (successResp(responseEntity)) {
//						result = getBody(responseEntity.getBody(), className);
//					} else {
//						handleRequestFailed(
//							responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
//						);
//					}
//
//					return result;
//				} catch (Exception e) {
//					retry++;
//					try {
//						Thread.sleep(retryInterval);
//					} catch (InterruptedException ignore) {
//					}
//				}
//			}
//			baseURL = changeBaseURLToNext(baseURL);
//			baseURLChangeTime++;
//		}
//
//		return null;
	}

	public <T> T postOne(Object obj, String resource, Class<T> className, String cookies) {
		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;
		setRetryTime();

		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource);
					ResponseEntity<ResponseBody> responseEntity;
					if (StringUtils.isEmpty(cookies)) {
						responseEntity = restTemplate.postForEntity(url, obj, ResponseBody.class);
					} else {
						HttpHeaders headers = new HttpHeaders();
						headers.add("Cookie", cookies);
						HttpEntity<Object> httpEntity = new HttpEntity<>(obj, headers);
						responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity, ResponseBody.class);
					}

					T result = null;
					if (successResp(responseEntity)) {
						responseBody = responseEntity.getBody();
						result = getBody(responseBody, className);
					} else {
						handleRequestFailed(url, HttpMethod.POST.name(), obj,
								responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
						);
					}

					return result;
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.POST.name(), obj, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg() + "data size: " + (obj.toString().getBytes().length / 1024 / 1024), exception.getMessage()
			), exception);
		} else {
			return null;
		}
	}

//    public  <T> T  postOne(Map<String, String> body, Class<T> className, String resource) {
//        StringBuilder sb = new StringBuilder(baseURL);
//        sb.append(resource);
//
//        restTemplate.get
//        ResponseEntity<ResponseEntity> responseEntity = restTemplate.postForEntity(sb.toString(), obj, ResponseEntity.class);
//        if (responseEntity.getStatusCode().is2xxSuccessful()) {
//            return true;
//        }
//
//        return false;
//    }

	public <T> T post(Map<String, Object> params, Object obj, String resource, Class<T> className) {

		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		URI url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = queryString(url(baseURL, resource + "/update"), params);
					responseBody = restTemplate.postForObject(url, obj, ResponseBody.class);
					T result = null;

					if (ResponseCode.SUCCESS.getCode().equals(responseBody.getCode())) {
						result = getBody(responseBody, className);
					} else {
						handleRequestFailed(url.toString(), HttpMethod.POST.name(), obj, responseBody);
					}

					return result;
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url != null ? url.toString() : null, HttpMethod.POST.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg() + "data size: " + (obj.toString().getBytes().length / 1024 / 1024), exception.getMessage()
			), exception);
		} else {
			return null;
		}
	}

	public <T> T upsert(Map<String, Object> params, Object obj, String resource, Class<T> className) {

		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		URI url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = queryString(url(baseURL, resource + "/upsertWithWhere"), params);

					responseBody = restTemplate.postForObject(url, obj, ResponseBody.class);
					T result = null;

					if (ResponseCode.SUCCESS.getCode().equals(responseBody.getCode())) {
						result = getBody(responseBody, className);
					} else {
						handleRequestFailed(url.toString(), HttpMethod.POST.name(), obj, responseBody);
					}

					return result;
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url != null ? url.toString() : null, HttpMethod.POST.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return null;
		}
	}

	public <T> List<T> getBatch(Map<String, Object> params, String resource, Class<T> className, String cookies) {
		return getBatch(params, resource, className, cookies, null);
	}

	public <T> List<T> getBatch(Map<String, Object> params, String resource, Class<T> className, String cookies, String region) {
		return getBatch(params, resource, className, cookies, region, null);
	}

	public <T> List<T> getBatch(Map<String, Object> params, String resource, Class<T> className, String cookies, String region, Predicate<?> stop) {

		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();

		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				if (null != stop && stop.test(null)) {
					break;
				}
				try {
					url = url(baseURL, resource);

					List<T> list = null;

					URI queryString = queryString(url, params);

					HttpEntity<String> httpEntity = null;

					if (StringUtils.isNotBlank(cookies)) {
						HttpHeaders headers = new HttpHeaders();
						if (StringUtils.isNotBlank(cookies)) {
							headers.add("Cookie", cookies);
						}
						if (StringUtils.isNotBlank(region)) {
							headers.add("jobTags", region);
						}
						httpEntity = new HttpEntity<>(headers);
					}

					ResponseEntity<ResponseBody> responseEntity = restTemplate.exchange(queryString, HttpMethod.GET, httpEntity, ResponseBody.class);

					if (successResp(responseEntity)) {
						responseBody = responseEntity.getBody();
						list = getListBody(responseBody, className);
					} else {
						// add patch, return empty list if the api does not exist
						if (responseEntity.hasBody() && "110400".equals(responseEntity.getBody().getCode())) {
							return new ArrayList<>();
						}
						handleRequestFailed(url, HttpMethod.GET.name(), httpEntity,
								responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
						);
					}

					return list;
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.GET.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return null;
		}
	}

	public <T> T getOne(Map<String, Object> params, String resource, Class<T> className, String cookies) {

		return getOne(params, resource, className, cookies, null);
	}

	public <T> T getOne(Map<String, Object> params, String resource, Class<T> className, String cookies, String region) {
		return getOne(params, resource, className, cookies, region, null);
	}

	public <T> T getOne(Map<String, Object> params, String resource, Class<T> className, String cookies, String region, Predicate<?> stop) {

		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();
		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				if (null != stop && stop.test(null)) {
					break;
				}
				T result = null;
				url = url(baseURL, resource);
				URI queryString = queryString(url, params);
				try {
					HttpEntity<String> httpEntity = null;

					if (StringUtils.isNotBlank(cookies)) {
						HttpHeaders headers = new HttpHeaders();
						if (StringUtils.isNotBlank(cookies)) {
							headers.add("Cookie", cookies);
						}
						if (StringUtils.isNotBlank(region)) {
							headers.add("jobTags", region);
						}
						httpEntity = new HttpEntity<>(headers);
					}

					ResponseEntity<ResponseBody> responseEntity = restTemplate.exchange(queryString, HttpMethod.GET, httpEntity, ResponseBody.class);

					if (successResp(responseEntity)) {
						responseBody = responseEntity.getBody();
						Object data = responseBody.getData();
						if (null == data) {
							return null;
						}
						if (data instanceof Map && ((Map) data).containsKey("items")) {
							Object items = ((Map) data).get("items");
							if (items instanceof List) {
								data = items;
							}
						}
						if (data instanceof List) {
							if (CollectionUtils.isNotEmpty((List) data)) {
								responseBody.setData(((List) data).get(0));
								result = getBody(responseBody, className);
							}
						} else {
							result = getBody(responseEntity.getBody(), className);
						}
					} else {
						handleRequestFailed(url, HttpMethod.GET.name(), httpEntity,
								responseEntity != null && responseEntity.hasBody() ? responseEntity.getBody() : null
						);
					}

					return result;
				} catch (RestDoNotRetryException e) {
					throw new RuntimeException("Query url: " + queryString, e);
				} catch (Exception e) {
					// 4xx 异常不进行重试
					if (e instanceof HttpClientErrorException) {
						if (String.valueOf(((HttpClientErrorException) e).getStatusCode().value()).startsWith("4")) {
							throw new RuntimeException("Query url: " + queryString, e);
						}
					}
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.GET.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return null;
		}
	}

	public File downloadFile(Map<String, Object> params, String resource, String path, String cookies, String region) {

		String baseURL = this.baseURL;
		int baseURLChangeTime = 0;
		Exception exception = null;

		setRetryTime();

		String url = null;
		ResponseBody responseBody = null;
		while (baseURLChangeTime < size) {
			int retry = 0;
			while (retry <= retryTime) {
				try {
					url = url(baseURL, resource);

					URI queryString = queryString(url, params);

					HttpEntity<String> httpEntity = null;

					if (StringUtils.isNotBlank(cookies)) {
						HttpHeaders headers = new HttpHeaders();
						if (StringUtils.isNotBlank(cookies)) {
							headers.add("Cookie", cookies);
						}
						if (StringUtils.isNotBlank(region)) {
							headers.add("jobTags", region);
						}
						headers.setAccept(
								ImmutableList.of(
										MediaType.APPLICATION_OCTET_STREAM,
										new MediaType("application", "*+json")
								)
						);
						httpEntity = new HttpEntity<>(headers);
					}

					ResponseEntity<Resource> responseEntity = restTemplate.exchange(queryString, HttpMethod.GET, httpEntity, Resource.class);
					if (MediaType.APPLICATION_OCTET_STREAM.includes(responseEntity.getHeaders().getContentType())) {
						File file = new File(path + ".bak");
						if(file.exists()) {
							FileUtils.deleteQuietly(file);
						}
						if(responseEntity.getBody() == null) {
							return null;
						}
						FileUtils.copyInputStreamToFile(responseEntity.getBody().getInputStream(), file);
						File realFile = new File(path);
						if(realFile.exists()) {
							FileUtils.deleteQuietly(realFile);
						}
						FileUtils.moveFile(file, realFile);
//						StreamUtils.copy(responseEntity.getBody().getInputStream(), new FileOutputStream(file));
						return file;
					} else {
						final Object body = responseEntity.getBody();
						responseBody = JSONUtil.json2POJO((String) body, ResponseBody.class);
						handleRequestFailed(url, HttpMethod.GET.name(), httpEntity,
								responseEntity != null && responseEntity.hasBody() ? responseBody : null
						);
						return null;
					}
				} catch (RestDoNotRetryException e) {
					throw e;
				} catch (HttpMessageConversionException e){
				    throw e;
				} catch (Exception e) {
					retry++;
					exception = retryExceptionHandle(e, url, HttpMethod.GET.name(), params, responseBody, retry);
				}
			}
			baseURL = changeBaseURLToNext(baseURL);
			baseURLChangeTime++;
		}

		if (exception != null) {
			throw new ManagementException(String.format(TapLog.ERROR_0006.getMsg(), exception.getMessage()), exception);
		} else {
			return null;
		}
	}

	public int getRetryTime() {
		return retryTime;
	}

	public List<String> getBaseURLs() {
		return baseURLs;
	}

	private String url(String baseURL, String resource) {

		StringBuilder sb = new StringBuilder(baseURL);
		return sb.append(resource).toString();
	}

	private URI queryString(String url, Map<String, ?> params) {
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url);

		if (MapUtils.isNotEmpty(params)) {
			for (Map.Entry<String, ?> entry : params.entrySet()) {
				builder.queryParam(entry.getKey(), UriUtils.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8));
			}
		}

		return builder.build(true).toUri();
	}

	private synchronized String changeBaseURLToNext(String baseURL) {
		int index = 0;

		for (int i = 0; i < size - 1; i++) {
			if (baseURL.equals(baseURLs.get(i))) {
				index = i + 1;
				break;
			}
		}

		this.baseURL = baseURLs.get(index);
		return baseURLs.get(index);
	}

	private boolean successResp(ResponseEntity<ResponseBody> responseEntity) {
		if (responseEntity == null) {
			return false;
		}

		if (!responseEntity.hasBody()) {
			return false;
		}

		return responseEntity.getStatusCode().is2xxSuccessful() && ResponseCode.SUCCESS.getCode().equals(responseEntity.getBody().getCode());
	}

	private void handleRequestFailed(String uri, String method, Object param, ResponseBody responseBody) throws JsonProcessingException {
		if (TmStatusService.isEnable()) {
			if((TmStatusService.isNotAvailable() ||
										(responseBody != null && StringUtils.containsAny(responseBody.getCode(), ResponseCode.UN_AVAILABLE.getCode())))) {
				if (logCount.incrementAndGet() % 1000 == 0) {
					logger.warn("tm unavailable...");
				}
				return;
			}
		}
		if (responseBody == null) {
			throw new ManagementException("Request management failed, response body is empty.");
		}

		logger.error("Request {} fail, error code {}, error message {}, request id {}",
				uri, responseBody.getCode(), responseBody.getMessage(), responseBody.getReqId());

		if (StringUtils.containsAny(responseBody.getCode(), "SystemError", "IllegalArgument", "Transition.Not.Supported")) {
			throw new RestDoNotRetryException(uri, method, param, responseBody);
		} else if ("110403".equals(responseBody.getCode())) {
			throw new RestAuthException(uri, method, param, responseBody);
		}

		throw new RestException(uri, method, param, responseBody);

//		throw new ManagementException(String.format("Request management failed, response body %s", JSONUtil.obj2Json(responseBody)));
	}

	private <T> T getBody(ResponseBody responseBody, Class<T> className) throws IOException {

		Object data = responseBody.getData();
		if (data != null) {
			return JSONUtil.json2POJO(JSONUtil.obj2Json(data), className);
		}

		return null;
	}

	private <T> List<T> getListBody(ResponseBody responseBody, Class<T> className) throws IOException {
		Object data = responseBody.getData();
		if (data instanceof List && CollectionUtils.isNotEmpty((List) data)) {
			return JSONUtil.json2List(JSONUtil.obj2Json(data), className);
		} else if (data instanceof Map) {
			Object itmes = ((Map) data).get("items");

			if (itmes != null && itmes instanceof List) {
				String json = JSONUtil.obj2Json(itmes);
				return JSONUtil.json2List(json, className);
			}
		}

		return null;
	}

	private void setRetryTime() {
		if (this.getRetryTimeout != null) {
			Long retryTimeout = getRetryTimeout.get();
			long avgRetryTimeout = retryTimeout % size != 0 ? (retryTimeout / size) + 1 : (retryTimeout / size);
			this.retryTime = (int) (avgRetryTimeout % retryInterval != 0 ? (avgRetryTimeout / retryInterval) + 1 : (avgRetryTimeout / retryInterval));
		}
	}

	enum ResponseCode {
		SUCCESS("ok"),
		UN_AVAILABLE("503"),
		;

		private String code;

		ResponseCode(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}
	}
}
