package com.tapdata.constant;

import java.util.Arrays;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-01-20 23:24
 **/
public class EngineConstant {

	/**
	 * 环境变量的key
	 */
	public final static String ENV_WORKER_DIR_KEY = "TAPDATA_WORK_DIR";
	public final static String ENV_IS_CLOUD_KEY = "isCloud";
	public final static String ENV_TAPDATA_MONGO_URI_KEY = "TAPDATA_MONGO_URI";
	public final static String ENV_TAPDATA_MONGO_CONN_KEY = "TAPDATA_MONGO_CONN";

	public static final String DEFAULT_TAPDATA_MONGO_URI = "mongodb://mongo:27017/tapdata";

	public static final List<String> DEFAULT_BASE_URLS = Arrays.asList("http://backend:3030/api/");
}
