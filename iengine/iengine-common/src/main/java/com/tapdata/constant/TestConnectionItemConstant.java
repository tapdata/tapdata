package com.tapdata.constant;

/**
 * @author samuel
 * @Description 用于返回给前端测试项，使用code，对接前端国际化
 * @create 2020-12-26 18:00
 **/
public class TestConnectionItemConstant {

	/**
	 * 通用项
	 */
	public final static String CHECK_CONNECT = "CHECK_CONNECT";
	public final static String CHECK_AUTH = "CHECK_AUTH";
	public final static String CHECK_VERSION = "CHECK_VERSION";
	public final static String LOAD_SCHEMA = "LOAD_SCHEMA";
	public final static String CHECK_CDC_PERMISSION = "CHECK_CDC_PERMISSION";
	public final static String CHECK_DDL_PERMISSION = "CHECK_DDL_PERMISSION";
	public final static String CHECK_PERMISSION = "CHECK_PERMISSION";
	public final static String CHECK_CONFIG = "CHECK_CONFIG";
	public final static String CHECK_READ_PERMISSION = "CHECK_READ_PERMISSION";

	/**
	 * oracle
	 */
	public final static String CHECK_ARCHIVE_LOG = "CHECK_ARCHIVE_LOG";
	public final static String CHECK_SUPPLEMENTAL_LOG = "CHECK_SUPPLEMENTAL_LOG";
	public final static String CHECK_REDO_LOG_PARSER = "CHECK_REDO_LOG_PARSER";

	/**
	 * mysql
	 */
	public final static String CHECK_BIN_LOG = "CHECK_BIN_LOG";
	public final static String CHECK_BIN_ROW_IMAGE = "CHECK_BIN_ROW_IMAGE";

	/**
	 * mysql pxc
	 */
	public final static String CHECK_GTID = "CHECK_GTID";
	public final static String CHECK_BIN_LOG_SYNC = "CHECK_BIN_LOG_SYNC";

	/**
	 * custom
	 */
	public final static String CHECK_SCRIPT = "CHECK_SCRIPT";
	public final static String CHECK_PRIMARY_KEY = "CHECK_PRIMARY_KEY";

	/**
	 * rest
	 */
	public final static String CHECK_ACCESS_TOKEN = "CHECK_ACCESS_TOKEN";
	public final static String CHECK_API_AUTH = "CHECK_API_AUTH";

	/**
	 * udp
	 */
	public final static String CHECK_LOCAL_PORT = "CHECK_LOCAL_PORT";

	/**
	 * file
	 */
	public final static String SCAN_FILE = "SCAN_FILE";
}
