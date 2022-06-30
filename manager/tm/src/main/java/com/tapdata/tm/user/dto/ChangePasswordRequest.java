/**
 * @title: ChangePasswordRequest
 * @description:
 * @author lk
 * @date 2021/12/10
 */
package com.tapdata.tm.user.dto;

import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ChangePasswordRequest {

	@NotBlank
	private String newPassword;

	@NotBlank
	private String oldPassword;

}
