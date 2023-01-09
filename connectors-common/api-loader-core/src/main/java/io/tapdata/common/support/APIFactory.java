package io.tapdata.common.support;

import java.util.Map;

/**
 * @author aplomb
 */
public interface APIFactory {
	String TYPE_POSTMAN = "postman";
	String TYPE_API_FOX = "api-fox";

	public static final String DEFAULT_POST_MAN_FILE_PATH = "connectors-javascript/coding-test-connector/src/main/resources/postman_api_collection.json";

	/**
	 * Generate APIInvoker instance from api description content.
	 *
	 * @param apiContent
	 * @param type could be postman or others
	 * @param params to replace the variables for global state
	 * @return
	 */
	APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params);

	APIInvoker loadAPI(Map<String, Object> params);

	APIInvoker loadAPI();
}
