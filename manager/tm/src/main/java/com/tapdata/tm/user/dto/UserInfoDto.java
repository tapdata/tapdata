package com.tapdata.tm.user.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/11/13 3:50 下午
 * @description
 */
@Getter
@Setter
public class UserInfoDto {

	private String id;
	private String username;
	private String userId;
	private String telephone;
	private String customId;

	private String token;

	private String customerId;

	private String customerType;
	private String email;


	private String nickname;
	private int userStatus;

	private Integer type;

	private String org;
	private String orgName;

	private Integer isCustomer;
	private Integer informType;

	private List<String> authoritys;

	public boolean isInternetAccount() {
		return Optional.ofNullable(this.customerType).filter(t -> t.contains("互联网")).isPresent();
	}

}
