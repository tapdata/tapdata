package io.tapdata.pdk.apis.api;

import java.util.Map;

/**
 * @author aplomb
 */
public interface APIInvoker {
	Map<String, Object> invoke(String uri, String method, Map<String, Object> params);
}
