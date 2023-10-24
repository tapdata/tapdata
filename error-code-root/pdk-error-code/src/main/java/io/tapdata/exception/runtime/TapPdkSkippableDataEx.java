package io.tapdata.exception.runtime;

import io.tapdata.PDKExCode_10;
import io.tapdata.exception.TapPdkBaseException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/26 11:39 Create
 */
public class TapPdkSkippableDataEx extends TapPdkBaseException {
    public TapPdkSkippableDataEx(String message, String pdkId, Throwable cause) {
        super(PDKExCode_10.SKIPPABLE_DATA, message, pdkId, cause);
    }

    @Override
    public String getMessage() {
        return String.format("Data failed in %s, %s", pdkId, super.getMessage());
    }
}
