package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-04-27 16:08
 **/
public class TapDbCdcConfigInvalidEx extends TapPdkBaseException {

    private String solutionSuggestions;

    public TapDbCdcConfigInvalidEx(String pdkId, Throwable cause) {
        super(PDKExCode_10.DB_CDC_CONFIG_INVALID, pdkId, cause);
    }

    public TapDbCdcConfigInvalidEx(String pdkId, String solutionSuggestions, Throwable cause) {
        super(PDKExCode_10.DB_CDC_CONFIG_INVALID, pdkId, cause);
        this.solutionSuggestions = solutionSuggestions;
    }

    @Override
    public String getMessage() {
        return "You need to do more configuration for the CDC of the data source, solution suggestions: " + solutionSuggestions;
    }

}
