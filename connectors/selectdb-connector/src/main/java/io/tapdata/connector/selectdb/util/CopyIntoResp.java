package io.tapdata.connector.selectdb.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CopyIntoResp implements Serializable {
  private CopyIntoResult result;
  
  private long time;
  
  public CopyIntoResult getResult() {
    return this.result;
  }
  
  public long getTime() {
    return this.time;
  }
  
  public void setResult(CopyIntoResult result) {
    this.result = result;
  }
  
  public void setTime(long time) {
    this.time = time;
  }
  
  public String toString() {
    return "CopyIntoResp{result=" + this.result + ", time=" + this.time + '}';
  }
}
