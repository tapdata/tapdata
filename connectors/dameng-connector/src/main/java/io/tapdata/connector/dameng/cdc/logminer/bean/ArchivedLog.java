package io.tapdata.connector.dameng.cdc.logminer.bean;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author huangjq
 */
public class ArchivedLog {
  /**
   * archived log location
   */
  private String name;

  /**
   * first_change scn
   */
  private long firstChangeScn;

  /**
   * next_change scn
   */
  private long nextChangeScn;

  public ArchivedLog() {
  }

  public ArchivedLog(String name, long firstChangeScn, long nextChangeScn) {
    this.name = name;
    this.firstChangeScn = firstChangeScn;
    this.nextChangeScn = nextChangeScn;
  }

  public ArchivedLog(ResultSet resultSet) throws SQLException {
    this.name = resultSet.getString("name");
    this.firstChangeScn = resultSet.getLong("first_change#");
    this.nextChangeScn = resultSet.getLong("next_change#");
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getFirstChangeScn() {
    return firstChangeScn;
  }

  public void setFirstChangeScn(long firstChangeScn) {
    this.firstChangeScn = firstChangeScn;
  }

  public long getNextChangeScn() {
    return nextChangeScn;
  }

  public void setNextChangeScn(long nextChangeScn) {
    this.nextChangeScn = nextChangeScn;
  }

  @Override
  public String toString() {
    return "ArchivedLog{" + "name='" + name + '\'' +
            ", firstChangeScn=" + firstChangeScn +
            '}';
  }
}
