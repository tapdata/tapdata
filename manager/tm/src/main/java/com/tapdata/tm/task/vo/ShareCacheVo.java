package com.tapdata.tm.task.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.tm.commons.task.dto.Status;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;


/**
 * Task
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class ShareCacheVo extends BaseVo {

  private String name;
  private String connectionName;
  private String connectionId;
  private String tableName;
  private String status;
  private List<Status> statuses;
  private Date createTime;
  private Date cacheTimeAt;

  private Integer maxMemory;

  @JsonInclude(JsonInclude.Include.ALWAYS)
  private String createUser;

  private String cacheKeys;
  private List<String> fields;
  private Long ttl;
  private Long maxRows;

  private String externalStorageId;
  private String externalStorageName;

}
