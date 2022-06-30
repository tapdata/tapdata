package com.tapdata.tm.commons.schema;


import lombok.Data;

import java.io.Serializable;

@Data
public class FileProperty implements Serializable {

  /**
   * include or exclude regex
   */
  private String include_filename;
  private String exclude_filename;

  private String file_type;

  /**
   * 多个文件合并的表名
   */
  private String file_schema;

  /**
   * excel addition
   */
  private String sheet_start;
  private String sheet_end;
  private String excel_header_type;
  private String excel_header_start;
  private String excel_header_end;
  private String excel_value_start;
  private String excel_value_end;
  private String excel_header_concat_char;
  private String excel_password;

  /**
   * csv, txt
   */
  private String seperate;

  /**
   * xml
   */
  private String data_content_xpath;

  /**
   * json
   */
  private String json_type;

  /**
   * 文件读取模式，内存模式/流模式
   */
  private String file_upload_mode;

  /**
   * data header when gridfs source excel/csv/txt
   * 1) specified_line: default
   * 2) custom:
   */
  private String gridfs_header_type;

  /**
   * 1）line number that header in (default 1), if gridfs_header_type is specified_line
   * 2) eg: name,age,email..., comma separate if gridfs_header_type is specified_line
   */
  private String gridfs_header_config;
}
