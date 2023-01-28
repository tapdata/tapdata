package io.tapdata.common.support;

import java.util.Map;

/**
 * @author aplomb
 */
public interface APIFactory {

    String DEFAULT_POST_MAN_FILE_PATH = "postman_api_collection.json";

    /**
     * Generate APIInvoker instance from api description content.
     *
     * @param apiContent
     * @param params     to replace the variables for global state
     * @return
     */
    APIInvoker loadAPI(String apiContent, Map<String, Object> params);

    APIInvoker loadAPI(Map<String, Object> params);

    APIInvoker loadAPI();
}
