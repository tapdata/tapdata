/**
 * @title: LoginRequest
 * @description:
 * @author lk
 * @date 2021/12/1
 */
package com.tapdata.tm.user.dto;

import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LoginRequest {

	@NotBlank
	private String email;

	@NotBlank
	private String password;

	private String sign;

	private String stime;
}
