package com.tapdata.tm.commons.schema;


import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class FileMeta implements Serializable {

  private String file_path;

  private String file_name;

  private String file_extension;

  private Long file_size_ondisk;

  private Long file_modify_time_ondisk;

  private String file_uri;

  private Long expired_unix_ts;

  private String server_addr;

  private Integer port;

  private String username;

  private String password;

  private List<String> tags;

  private Map<String, String> custom_meta;

  private String file_protocol;

  private Integer version;

  private Integer ttl;

  private Long file_create_time_ondisk;

  private String source_path;

  private String gridfsId;

  private Long length;

  private String fileType;

  private String connId;

  /**
   * 多文件，合并为一个文件时，产生的新的统一模型，与原来多个文件的映射
   */
  private List<FileMeta> fromFile;
}
