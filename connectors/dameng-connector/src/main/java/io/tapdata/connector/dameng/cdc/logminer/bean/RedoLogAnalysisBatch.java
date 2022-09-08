package io.tapdata.connector.dameng.cdc.logminer.bean;

import java.util.ArrayList;
import java.util.List;

public class RedoLogAnalysisBatch implements Comparable<RedoLogAnalysisBatch> {

  private String status;

  private long startSCN;

  private String startTime;

  private List<RedoLog> redoLogs = new ArrayList<>();

  public RedoLogAnalysisBatch() {
  }

  public long getStartSCN() {
    return startSCN;
  }

  public void setStartSCN(long startSCN) {
    this.startSCN = startSCN;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public List<RedoLog> getRedoLogs() {
    return redoLogs;
  }

  public void setRedoLogs(List<RedoLog> redoLogs) {
    this.redoLogs = redoLogs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("RedoLogAnalysisBatch{");
    sb.append("status='").append(status).append('\'');
    sb.append(", startSCN=").append(startSCN);
    sb.append(", startTime='").append(startTime).append('\'');
    sb.append(", redoLogs=").append(redoLogs);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public int compareTo(RedoLogAnalysisBatch o) {
    if (o == null) {
      return 1;
    }
    long startSCN = o.getStartSCN();
    if (this.getStartSCN() < startSCN) {
      return -1;
    } else if (this.getStartSCN() > startSCN) {
      return 1;
    }
    return 0;
  }
}
