/**
 * @title: MessageQueue
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.messagequeue;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("MessageQueue")
@Data
@EqualsAndHashCode(callSuper=false)
public class MessageQueue extends BaseEntity {

	private String type;

	private Object data;

	private String sender;

	private String receiver;

}
