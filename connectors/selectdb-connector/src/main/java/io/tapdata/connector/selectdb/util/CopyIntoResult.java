package io.tapdata.connector.selectdb.util;

import java.io.Serializable;

public class CopyIntoResult implements Serializable {
  public static final String FINISHED_STATE = "FINISHED";
  
  private String id;
  
  private String msg;
  
  private String loadedRows;
  
  private String filterRows;
  
  private String unselectRows;
  
  private String url;
  
  private String state;
  
  private String type;
  
  public String getId() {
    return this.id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public String getMsg() {
    return this.msg;
  }
  
  public void setMsg(String msg) {
    this.msg = msg;
  }
  
  public String getLoadedRows() {
    return this.loadedRows;
  }
  
  public void setLoadedRows(String loadedRows) {
    this.loadedRows = loadedRows;
  }
  
  public String getFilterRows() {
    return this.filterRows;
  }
  
  public void setFilterRows(String filterRows) {
    this.filterRows = filterRows;
  }
  
  public String getUnselectRows() {
    return this.unselectRows;
  }
  
  public void setUnselectRows(String unselectRows) {
    this.unselectRows = unselectRows;
  }
  
  public String getUrl() {
    return this.url;
  }
  
  public void setUrl(String url) {
    this.url = url;
  }
  
  public String getState() {
    return this.state;
  }
  
  public void setState(String state) {
    this.state = state;
  }
  
  public String getType() {
    return this.type;
  }
  
  public void setType(String type) {
    this.type = type;
  }
  
  public String toString() {
    return "CopyIntoResult{id='" + this.id + '\'' + ", msg='" + this.msg + '\'' + ", loadedRows='" + this.loadedRows + '\'' + ", filterRows='" + this.filterRows + '\'' + ", unselectRows='" + this.unselectRows + '\'' + ", url='" + this.url + '\'' + ", state='" + this.state + '\'' + ", type='" + this.type + '\'' + '}';
  }
}
