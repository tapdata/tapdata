package com.tapdata.tm.base.entity;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 3:19 下午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
public class BaseEntity extends Entity implements Serializable {

	/**
	 * 租户ID
	 */
	@Indexed
	private String customId;

	@Indexed
	@Field("createTime")
	private Date createAt;
	@Indexed
	@Field("last_updated")
	private Date lastUpdAt;


	/**
	 * 对应操作该条记录的当前用户的id
	 */
	@Indexed
	@Field("user_id")
	private String userId;
	@Indexed
	private String lastUpdBy;
	@Indexed
	private String createUser;
}
