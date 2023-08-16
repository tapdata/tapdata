package com.tapdata.tm.commons.base.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 9:22 上午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class BaseDto implements Serializable {

	@JsonSerialize( using = ObjectIdSerialize.class)
	@JsonDeserialize( using = ObjectIdDeserialize.class)
	private ObjectId id;

	private String customId;

	@JsonProperty("createTime")
	private Date createAt;

	@JsonProperty("last_updated")
//	@JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
	private Date lastUpdAt;

	@JsonProperty("user_id")
	private String userId;
	private String lastUpdBy;
	private String createUser;

	// 数据授权配置，需要配合 IDataPermissionDto 使用
	private Set<String> permissionActions;
}
