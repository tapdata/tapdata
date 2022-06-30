/**
 * @title: CustomMonitorInfo
 * @description:
 * @author lk
 * @date 2021/12/7
 */
package com.tapdata.tm.cluster.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;

@Setter
@Getter
public class CustomMonitorInfo {

	@JsonSerialize( using = ObjectIdSerialize.class)
	@JsonDeserialize( using = ObjectIdDeserialize.class)
	private ObjectId id;

	private String arguments;

	private String command;

	private String name;

	private String uuid;
}
