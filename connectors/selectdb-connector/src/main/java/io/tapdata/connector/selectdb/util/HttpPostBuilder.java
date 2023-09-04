package io.tapdata.connector.selectdb.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HttpPostBuilder {
  String url;
  
  Map<String, String> header = new HashMap<>();
  
  HttpEntity httpEntity;
  
  public HttpPostBuilder setUrl(String url) {
    this.url = url;
    return this;
  }
  
  public HttpPostBuilder addCommonHeader() {
    this.header.put("Expect", "100-continue");
    return this;
  }
  
  public HttpPostBuilder baseAuth(String user, String password) {
    String authInfo = user + ":" + password;
    byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
    this.header.put("Authorization", "Basic " + new String(encoded));
    return this;
  }
  
  public HttpPostBuilder setEntity(HttpEntity httpEntity) {
    this.httpEntity = httpEntity;
    return this;
  }
  
  public HttpPost build() {
    HttpPost put = new HttpPost(this.url);
    this.header.forEach(put::setHeader);
    put.setEntity(this.httpEntity);
    return put;
  }
}
