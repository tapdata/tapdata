package com.tapdata.constant;

import com.tapdata.entity.Connections;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-01-07 12:23
 **/
public class FileProperty implements Serializable {

	private static final long serialVersionUID = -1697693053485249898L;

	private static Map<String, String> fileTypeToFileSuffixRegexMap = new HashMap<>();

	static {
		fileTypeToFileSuffixRegexMap.put("xml", "xml");
		fileTypeToFileSuffixRegexMap.put("excel", "(xls|xlsx)");
		fileTypeToFileSuffixRegexMap.put("csv", "(csv|txt)");
		fileTypeToFileSuffixRegexMap.put("json", "");
	}

	public final static String JSON_ARRAY_BEGIN = "ArrayBegin";
	public final static String JSON_OBJECT_BEGIN = "ObjectBegin";

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
	private String excel_header_type = Connections.EXCEL_HEADER_TYPE_VALUE;
	private String excel_header_start;
	private String excel_header_end;
	private String excel_value_start;
	private String excel_value_end;
	private String excel_header_concat_char = "-";
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
	private String file_upload_mode = ConnectorConstant.GET_FILE_IN_STREAM;

	/**
	 * data header when gridfs source excel/csv/txt
	 * 1) specified_line: default
	 * 2) custom:
	 */
	private String gridfs_header_type = Connections.GRIDFS_HEADER_TYPE_SPECIFIED_LINE;

	/**
	 * 1）line number that header in (default 1), if gridfs_header_type is specified_line
	 * 2) eg: name,age,email..., comma separate if gridfs_header_type is specified_line
	 */
	private String gridfs_header_config = "1";

	public FileProperty() {
	}

	public String getInclude_filename() {
		return include_filename;
	}

	public void setInclude_filename(String include_filename) {
		this.include_filename = include_filename;
	}

	public String getExclude_filename() {
		return exclude_filename;
	}

	public void setExclude_filename(String exclude_filename) {
		this.exclude_filename = exclude_filename;
	}

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public String getFile_schema() {
		return file_schema;
	}

	public void setFile_schema(String file_schema) {
		this.file_schema = file_schema;
	}

	public String getSheet_start() {
		return sheet_start;
	}

	public void setSheet_start(String sheet_start) {
		this.sheet_start = sheet_start;
	}

	public String getSheet_end() {
		return sheet_end;
	}

	public void setSheet_end(String sheet_end) {
		this.sheet_end = sheet_end;
	}

	public String getExcel_header_type() {
		return excel_header_type;
	}

	public void setExcel_header_type(String excel_header_type) {
		this.excel_header_type = excel_header_type;
	}

	public String getExcel_header_start() {
		return excel_header_start;
	}

	public void setExcel_header_start(String excel_header_start) {
		this.excel_header_start = excel_header_start;
	}

	public String getExcel_header_end() {
		return excel_header_end;
	}

	public void setExcel_header_end(String excel_header_end) {
		this.excel_header_end = excel_header_end;
	}

	public String getExcel_value_start() {
		return excel_value_start;
	}

	public void setExcel_value_start(String excel_value_start) {
		this.excel_value_start = excel_value_start;
	}

	public String getExcel_value_end() {
		return excel_value_end;
	}

	public void setExcel_value_end(String excel_value_end) {
		this.excel_value_end = excel_value_end;
	}

	public String getExcel_header_concat_char() {
		return excel_header_concat_char;
	}

	public void setExcel_header_concat_char(String excel_header_concat_char) {
		this.excel_header_concat_char = excel_header_concat_char;
	}

	public String getSeperate() {
		return seperate;
	}

	public void setSeperate(String seperate) {
		this.seperate = seperate;
	}

	public String getData_content_xpath() {
		return data_content_xpath;
	}

	public void setData_content_xpath(String data_content_xpath) {
		this.data_content_xpath = data_content_xpath;
	}

	public String getJson_type() {
		return json_type;
	}

	public void setJson_type(String json_type) {
		this.json_type = json_type;
	}

	public String getFile_upload_mode() {
		return file_upload_mode;
	}

	public void setFile_upload_mode(String file_upload_mode) {
		this.file_upload_mode = file_upload_mode;
	}

	public String getGridfs_header_type() {
		return gridfs_header_type;
	}

	public void setGridfs_header_type(String gridfs_header_type) {
		this.gridfs_header_type = gridfs_header_type;
	}

	public String getGridfs_header_config() {
		return gridfs_header_config;
	}

	public void setGridfs_header_config(String gridfs_header_config) {
		this.gridfs_header_config = gridfs_header_config;
	}

	public String getExcel_password() {
		return excel_password;
	}

	public void setExcel_password(String excel_password) {
		this.excel_password = excel_password;
	}

	public void init() {
		String suffix = fileTypeToFileSuffixRegexMap.get(file_type);
		if (StringUtils.isBlank(suffix)) {
			return;
		}
		if (StringUtils.isNotBlank(include_filename)) {
			if (needAddSuffix(include_filename)) {
				include_filename += "\\." + suffix;
			}
		} else {
			this.include_filename = ".*\\." + suffix;
		}

		if (StringUtils.isNotBlank(exclude_filename)) {
			if (needAddSuffix(exclude_filename)) {
				exclude_filename += "\\." + suffix;
			}
		}
	}

	private boolean needAddSuffix(String fileNameFilter) {
		String suffix = fileTypeToFileSuffixRegexMap.get(file_type);
		boolean needAddSuffix = false;
		switch (file_type) {
			case "excel":
				if (!StringUtils.endsWithAny(fileNameFilter, ".xlsx", ".xls", "." + suffix)) {
					needAddSuffix = true;
				}
				break;
			case "xml":
				if (!StringUtils.endsWithAny(fileNameFilter, "." + suffix)) {
					needAddSuffix = true;
				}
				break;
			case "csv":
				if (!StringUtils.endsWithAny(fileNameFilter, ".csv", ".txt", "." + suffix)) {
					needAddSuffix = true;
				}
				break;
			default:
				break;
		}
		return needAddSuffix;
	}

	@Override
	public String toString() {
		return "FileProperty{" +
				"include_filename='" + include_filename + '\'' +
				", exclude_filename='" + exclude_filename + '\'' +
				", file_type='" + file_type + '\'' +
				", file_schema='" + file_schema + '\'' +
				", sheet_start='" + sheet_start + '\'' +
				", sheet_end='" + sheet_end + '\'' +
				", excel_header_type='" + excel_header_type + '\'' +
				", excel_header_start='" + excel_header_start + '\'' +
				", excel_header_end='" + excel_header_end + '\'' +
				", excel_value_start='" + excel_value_start + '\'' +
				", excel_value_end='" + excel_value_end + '\'' +
				", excel_header_concat_char='" + excel_header_concat_char + '\'' +
				", excel_password='" + excel_password + '\'' +
				", seperate='" + seperate + '\'' +
				", data_content_xpath='" + data_content_xpath + '\'' +
				", json_type='" + json_type + '\'' +
				", file_upload_mode='" + file_upload_mode + '\'' +
				", gridfs_header_type='" + gridfs_header_type + '\'' +
				", gridfs_header_config='" + gridfs_header_config + '\'' +
				'}';
	}
}
