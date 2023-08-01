package com.tapdata.tm.base.exception;

import com.tapdata.tm.utils.MessageUtil;
import lombok.Getter;
import lombok.Setter;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/13 2:42 下午
 * @description
 */
@Getter
@Setter
public class BizException extends RuntimeException {

	public static final String SYSTEM_ERROR = "SystemError";
	private String errorCode;
	private Object[] args;

	public BizException (String errorCode){
		super(MessageUtil.getMessage(errorCode));
		args = new Object[]{errorCode};
		this.errorCode = this.getMessage().equals(errorCode) ? SYSTEM_ERROR : errorCode;
	}

	public BizException (String errorCode, Object... args){
		super(MessageUtil.getMessage(errorCode, args));
		this.errorCode = errorCode;
		this.args = args;
	}

	public BizException (String errorCode, Throwable cause, Object... args){
		super(MessageUtil.getMessage(errorCode, args), cause);
		this.errorCode = errorCode;
		this.args = args;
	}
	public BizException (Throwable cause){
		super(MessageUtil.getMessage("SystemError", cause.getMessage()), cause);
		this.errorCode = "SystemError";
		this.args = new Object[]{cause.getMessage()};
	}

}
