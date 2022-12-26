package io.tapdata.pdk.apis.javascript;

import java.util.Map;

/**
 * @author aplomb
 */
public interface APIFactory {
	String TYPE_POSTMAN = "postman";

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
