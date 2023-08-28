package io.tapdata.connector.selectdb.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseResponse<T> implements Serializable {
  private int code;
  
  private String msg;
  
  private T data;
  
  public BaseResponse() {}
  
  public BaseResponse(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }
  
  public int getCode() {
    return this.code;
  }
  
  public String getMsg() {
    return this.msg;
  }
  
  public T getData() {
    return this.data;
  }
  
  public void setCode(int code) {
    this.code = code;
  }
  
  public void setMsg(String msg) {
    this.msg = msg;
  }
  
  public void setData(T data) {
    this.data = data;
  }
  
  public static BaseResponse fail(int code, String message) {
    return new BaseResponse(code, message);
  }
  
  public String toString() {
    return "BaseResponse{code=" + this.code + ", msg='" + this.msg + '\'' + ", data=" + this.data + '}';
  }
}
