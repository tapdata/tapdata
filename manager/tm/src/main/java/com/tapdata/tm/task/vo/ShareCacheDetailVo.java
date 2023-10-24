package com.tapdata.tm.task.vo;

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
public class ShareCacheDetailVo extends BaseVo {

  private String id;
  private String name;
  private String connectionName;
  private String tableName;
  private String connectionId;
  private String status;
  private Long ttl;
  private Date createTime;
  private Date cacheTimeAt;
  private String createUser;
  private String cacheKeys;
  private List<String> fields;
  private Long maxRows;
  private Integer maxMemory;

  private String externalStorageId;

}
