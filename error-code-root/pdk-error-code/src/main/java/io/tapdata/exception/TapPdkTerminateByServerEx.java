package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

/**
 * @author samuel
 * @Description
 * @create 2023-03-17 21:42
 **/
public class TapPdkTerminateByServerEx extends TapPdkBaseException {

	private static final long serialVersionUID = 420616107958068224L;

	public TapPdkTerminateByServerEx(String pdkId, Throwable cause) {
		super(PDKExCode_10.TERMINATE_BY_SERVER, pdkId, cause);
	}

	@Override
	public String getMessage() {
		return String.format("The client connection was terminated by the %s server", pdkId);
	}
}
