/**
 * @title: JobConnection
 * @description:
 * @author lk
 * @date 2021/12/21
 */
package com.tapdata.tm.job.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

@Getter
@Setter
public class JobConnection {

	@JsonSerialize( using = ObjectIdSerialize.class)
	@JsonDeserialize( using = ObjectIdDeserialize.class)
	@Field(targetType = FieldType.OBJECT_ID)
	private ObjectId source;

	@JsonSerialize( using = ObjectIdSerialize.class)
	@JsonDeserialize( using = ObjectIdDeserialize.class)
	@Field(targetType = FieldType.OBJECT_ID)
	private ObjectId target;

	/**
	 * 标记目标是否为缓存节点
	 * 缓存节点在connections中不会有对应的连接信息，需要单独处理
	 */
	private Boolean cacheTarget;
}
