package com.tapdata.tm.sdk.util;


import com.tapdata.tm.sdk.auth.BasicCredentials;
import com.tapdata.tm.sdk.auth.Signer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 云环境请求签名工具类
 */
public class CloudSignUtil {

	private static final Logger logger = LogManager.getLogger(CloudSignUtil.class);

	private final static boolean needSign;
	private final static String accessKey;
	private final static String secretKey;

	static {
		accessKey = EnvUtil.get("accessKey");
		secretKey = EnvUtil.get("secretKey");
		needSign = AppType.init().isCloud() && !StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey);
		logger.info("ak/sk needSign {}, accessKey {}, secretKey {}", needSign, accessKey, secretKey);
	}

	public static void main(String[] args) {
//    String url = "ws://127.0.0.1:3030/ws/agent?agentId=075d56b7-3870-4e18-9455-98b450f89ad7&access_token=HxmMjEeCCUQs6COtPmp0XcaEkR6k2wkOJva1lb7AKvwNQ1GwJnwf5TAklmj5z8Fu";
//    String url = "ws://127.0.0.1:3030/ws/agent";
		String url = "http://test.cloud.tapdata.net/tm/api/Settings";
		System.out.println(getQueryStr("GET", url, ""));

		String url1 = "http://test.cloud.tapdata.net/tm/api/Logs";
		String body = "{\n" +
						"  \"level\": \"ERROR\",\n" +
						"  \"loggerName\": \"com.tapdata.validator.SchemaValidatorImpl\",\n" +
						"  \"message\": \"Load schema error, schema name: ORACLE_NCLOB_TYPE, err msg: Invalid column name, will skip it\",\n" +
						"  \"threadId\": 22423,\n" +
						"  \"threadName\": \"LOAD-SCHEMA-FIELDS-[DM2]\",\n" +
						"  \"threadPriority\": 5,\n" +
						"  \"millis\": 1628478201231,\n" +
						"  \"date\": 1628478201231,\n" +
						"  \"thrown\": {\n" +
						"    \"type\": \"java.sql.SQLException\",\n" +
						"    \"message\": \"Invalid column name\\ndm.jdbc.dbaccess.DBError.throwSQLException(DBError.java:57)\\ndm.jdbc.driver.DmdbResultSet_bs.findColumn(DmdbResultSet_bs.java:1938)\\ndm.jdbc.driver.DmdbResultSet_bs.getString(DmdbResultSet_bs.java:1239)\\ndm.jdbc.driver.DmdbResultSet.do_getString(DmdbResultSet.java:5133)\\ndm.jdbc.filter.FilterChain.ResultSet_getString(FilterChain.java:4848)\\ndm.jdbc.driver.DmdbResultSet.getString(DmdbResultSet.java:637)\\ncom.tapdata.validator.SchemaValidatorImpl.validateSchema(SchemaValidatorImpl.java:176)\\ncom.tapdata.validator.mysql.MysqlSchemaValidatorImpl.validateSchema(MysqlSchemaValidatorImpl.java:29)\\ncom.tapdata.validator.SchemaValidatorImpl.validateSchema(SchemaValidatorImpl.java:51)\\ncom.tapdata.validator.SchemaFactory.loadSchemaList(SchemaFactory.java:86)\\nio.tapdata.Runnable.LoadSchemaRunner.run(LoadSchemaRunner.java:89)\\nio.tapdata.websocket.handler.TestConnectionHandler.lambda$handle$3(TestConnectionHandler.java:300)\\njava.lang.Thread.run(Thread.java:745)\\n\"\n" +
						"  },\n" +
						"  \"contextMap\": {},\n" +
						"  \"contextStack\": []\n" +
						"}";
		System.out.println(getQueryStr("POST", url1, body));
	}

	public static boolean isNeedSign() {
		return needSign;
	}

	public static String getQueryStr(String reqMethod, String url) {
		return getQueryStr(reqMethod, url, "");
	}

	public static String getQueryStr(String reqMethod, String url, String bodyStr) {
		String[] urlArr = splitUrl(url);
		Map<String, String> paramMap = urlParamSplit(urlArr[1]);
		//添加公共参数
		paramMap.put("ts", String.valueOf(System.currentTimeMillis()));
		paramMap.put("nonce", UUID.randomUUID().toString());
		paramMap.put("signVersion", "1.0");
		paramMap.put("accessKey", accessKey);

		String strToSign = SignUtil.canonicalQueryString(paramMap);
		if (!StringUtils.isEmpty(reqMethod)) {
			strToSign = String.format("%s:%s", reqMethod, strToSign);
		}
		/**
		 * 2. 有Request Body且 Content-Type为 application/json：
		 * StringToSign = HTTPRequestMethod + ":" + CanonicalQueryString + ":" + RequestPayload
		 */
		if (!url.startsWith("ws")) {
			strToSign = String.format("%s:%s", strToSign, bodyStr);
		}
		BasicCredentials credentials = new BasicCredentials(accessKey, secretKey);
		Signer signer = Signer.getSigner(credentials);
		String sign = signer.signString(strToSign, credentials);
		paramMap.put("sign", sign);

		String queryStr = urlArr[0] + "?" + paramMap.entrySet().stream().map(CloudSignUtil::percentEncode).collect(Collectors.joining("&"));
		logger.debug("cloud sign: url=[{}], requestMethod=[{}], body=[{}], signToStr=[{}], \n query: [{}]",
						url, reqMethod, bodyStr, strToSign, queryStr);
		return queryStr;
	}

	/**
	 * 分离url的路径和参数
	 *
	 * @param url
	 * @return
	 */
	public static String[] splitUrl(String url) {
		String[] retArr = new String[2];
		int splitIndex = url.contains("?") ? url.indexOf("?") : url.length();
		retArr[0] = url.substring(0, splitIndex);
		retArr[1] = url.substring(splitIndex == url.length() ? splitIndex : splitIndex + 1);
		return retArr;
	}

	/**
	 * 将参数存入map集合
	 *
	 * @param strUrlParam url参数
	 * @return url请求参数部分存入map集合
	 */
	public static Map<String, String> urlParamSplit(String strUrlParam) {
		Map<String, String> mapRequest = new HashMap<>();
		if (strUrlParam == null) {
			return mapRequest;
		}
		String[] arrSplit = strUrlParam.split("[&]");
		for (String strSplit : arrSplit) {
			String[] arrSplitEqual = strSplit.split("[=]");
			//解析出键值
			if (arrSplitEqual.length > 1) {
				//正确解析
				mapRequest.put(arrSplitEqual[0], UriUtils.decode(arrSplitEqual[1], StandardCharsets.UTF_8));
			} else {
				if (!arrSplitEqual[0].equals("")) {
					//只有参数没有值，不加入
					mapRequest.put(arrSplitEqual[0], "");
				}
			}
		}
		return mapRequest;
	}

	private static String percentEncode(Map.Entry<String, String> e) {
		try {
			return String.format("%s=%s", SignUtil.percentEncode(e.getKey()), SignUtil.percentEncode(e.getValue()));
		} catch (UnsupportedEncodingException unsupportedEncodingException) {
			logger.error("percentEncode error {}", unsupportedEncodingException.getMessage());
		}
		return e.getKey() + "=" + e.getValue();
	}
}
