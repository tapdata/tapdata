package com.tapdata.tm.commons.ping;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.util.ErrorUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author samuel
 * @Description
 * @create 2022-11-17 11:00
 **/
@Getter
@Setter
@ToString
public class PingDto extends BaseDto {

	public static final String PING_RESULT = "pingResult";
	public static final String ERR_MESSAGE = "errMessage";

	private String pingId;
	private PingType pingType;
	private Object data;
	private PingResult pingResult;
	private String errMessage;

	public void fail(Throwable throwable) {
		this.pingResult = PingResult.FAIL;
		if (null != throwable) {
			this.errMessage = throwable.getMessage() + "\n" + ErrorUtil.getStackString(throwable);
		}
	}

	public void fail(String errMessage) {
		this.pingResult = PingResult.FAIL;
		this.errMessage = errMessage;
	}

	public void ok() {
		this.pingResult = PingResult.OK;
	}

	public enum PingResult {
		OK,
		FAIL,
	}
}
