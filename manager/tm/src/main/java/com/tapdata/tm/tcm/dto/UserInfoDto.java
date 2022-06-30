package com.tapdata.tm.tcm.dto;

import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

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

	// authing's field
	private String status;
	private String userPoolId;
	private Boolean emailVerified;
	private Boolean phoneVerified;
	private String unionid;
	private String openid;
	private String identities;
	private String registerSource;
	private String photo;
	private String password;
	private int loginsCount;
	private String lastLogin;
	private String lastIP;
	private String signedUp;
	private Boolean blocked;
	private Boolean isDeleted;
	private String locale;
	private String device;
	private String browser;
	private String company;
	private String givenName;
	private String familyName;
	private String middleName;
	private String profile;
	private String preferredUsername;
	private String website;
	private String gender;
	private String birthdate;
	private String zoneinfo;
	private String address;
	private String formatted;
	private String streetAddress;
	private String locality;
	private String region;
	private String postalCode;
	private String city;
	private String province;
	private String country;


	private Integer isApproval; //  是否需要审批	1-审批用户，0-非审批用户


	private Boolean internetAccount;

}
