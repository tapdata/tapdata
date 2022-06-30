/**
 * @title: CreateUserRequest
 * @description:
 * @author lk
 * @date 2021/12/9
 */
package com.tapdata.tm.user.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CreateUserRequest {

	private String accesscode;

	@JsonProperty("account_status")
	private Integer accountStatus;

	@NotBlank
	private String email;

	private Boolean emailVerified;

	@JsonProperty("emailVerified_from_frontend")
	private Boolean emailVerifiedFromFrontend;

	private String password;

	private List<Object> roleusers;

	private String source;

	private String status;

	private String username;
}
