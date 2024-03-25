package com.tapdata.tm.base.dto.ds;

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
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 9:22 上午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class DsBaseDto {

	@JsonSerialize( using = ObjectIdSerialize.class)
	@JsonDeserialize( using = ObjectIdDeserialize.class)
	private ObjectId id;

	@Indexed
	private String customId;


	@Indexed
	@JsonProperty("createTime")
	private String createAt;
	@Indexed
	@JsonProperty("last_updated")
	private Date lastUpdAt;
	@Indexed
	@JsonProperty("user_id")
	private String userId;
	@Indexed
	private String lastUpdBy;
	@Indexed
	private String createUser;
}
