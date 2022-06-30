/**
 * @title: MessageQueueDto
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.messagequeue.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MessageQueueDto extends BaseDto {

	private String type;

	private Object data;

	private String sender;

	private String receiver;

}
