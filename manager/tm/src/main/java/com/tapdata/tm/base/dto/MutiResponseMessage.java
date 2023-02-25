package com.tapdata.tm.base.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 9:24 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class MutiResponseMessage {

	private String reqId;

	/**
	 * 请求处理的代码
	 */
	protected String code = ResponseMessage.OK;

	/**
	 * 请求处理失败时的错误消息
	 */
	protected String message;
	private String id;
}
