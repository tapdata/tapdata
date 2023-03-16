package io.tapdata.pdk.apis.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 12:08
 **/
@TapExClass(code = 10, module = "PDK", describe = "Error code for PDK")
public interface PDKExCode_10 {
	@TapExCode
	String BATCH_READ_MISSING_PRIVILEGES = "10001";
	@TapExCode
	String STREAM_READ_MISSING_PRIVILEGES = "10002";
	@TapExCode
	String USERNAME_PASSWORD_INVALID = "10003";
	@TapExCode
	String OFFSET_OUT_OF_LOG = "10004";
}
