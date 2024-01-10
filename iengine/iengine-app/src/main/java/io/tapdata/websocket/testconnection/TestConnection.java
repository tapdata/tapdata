package io.tapdata.websocket.testconnection;

import com.tapdata.validator.ConnectionValidateResult;

import java.util.Map;

public interface TestConnection {

    void testConnection(Map event,ConnectionValidateResult connectionValidateResult);

}
