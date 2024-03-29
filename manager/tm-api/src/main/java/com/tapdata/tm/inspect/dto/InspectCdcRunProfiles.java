package com.tapdata.tm.inspect.dto;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;

/**
 * 数据检验 - 增量运行配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 下午4:27 Create
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectCdcRunProfiles implements Serializable {

  private Instant cdcBeginDate; // 增量开始时间
  private Instant cdcEndDate; // 增量结束时间，调度启动时无效
  private int winDuration; // 窗口时长
  private String sourceOffset; // 记录开始位置 offset
  private String targetOffset; // 记录开始位置 offset

  public InspectCdcRunProfiles() {
  }

  public InspectCdcRunProfiles(Instant cdcBeginDate, int winDuration) {
    this.cdcBeginDate = cdcBeginDate;
    this.winDuration = winDuration;
  }

  @JsonGetter("cdcBeginDate")
  public Long jacksonSetCdcBeginDate() {
    if (null == cdcBeginDate) return null;
    return cdcBeginDate.toEpochMilli();
  }

  @JsonSetter("cdcBeginDate")
  public void jacksonGetCdcBeginDate(Long val) {
    if (null != val) {
      cdcBeginDate = Instant.ofEpochMilli(val);
    }
  }

  @JsonGetter("cdcEndDate")
  public Long jacksonSetCdcEndDate() {
    if (null == cdcEndDate) return null;
    return cdcEndDate.toEpochMilli();
  }

  @JsonSetter("cdcEndDate")
  public void jacksonGetCdcEndDate(Long val) {
    if (null != val) {
      cdcEndDate = Instant.ofEpochMilli(val);
    }
  }
}
