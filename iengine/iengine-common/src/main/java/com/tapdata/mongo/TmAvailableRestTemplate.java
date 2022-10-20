package com.tapdata.mongo;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.ResponseBody;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.AbstractClientHttpResponse;
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

public class TmAvailableRestTemplate extends RestTemplate {

  public TmAvailableRestTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
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
        ResponseBody responseBody = new ResponseBody();
        responseBody.setCode("503");
        responseBody.setMsg("503 Service Unavailable");
        String obj2Json = JSONUtil.obj2Json(responseBody);
        byte[] bytes = obj2Json.getBytes(StandardCharsets.UTF_8);
        return new ByteArrayInputStream(bytes);
      }

      @Override
      public HttpHeaders getHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        return httpHeaders;
      }
    };
  }

}
