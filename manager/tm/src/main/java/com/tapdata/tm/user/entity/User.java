package com.tapdata.tm.user.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/11 下午8:05
 * @description
 */
@Data
@EqualsAndHashCode(callSuper=false)
@Document("User")
public class User extends BaseEntity {

    @Field("accesscode")
    @Indexed(unique = true)
    private String accessCode;
    private String username;
    private String password;
    @Indexed(unique = true)
    private String email;
    private String phone;
    private boolean emailVerified;

    @Field("emailVerified_from_frontend")
    private Boolean emailVerifiedFromFrontend;
    /**
     * 外部用户id ，对应tcm返回的userInfoDto的 userId
     */
    @Field("userId")
    @Indexed(unique = true)
    private String externalUserId; // authing or eCloud user id

    private String isPrimary;

    private boolean isCompleteGuide;

    @Field("account_status")
    private int accountStatus;

    private Notification notification;
    private GuideData guideData;


    private Integer isCustomer;

    private String nickname;
    // authing's field,对应的是在author里的状态   "status": "Activated",

    //用customId
    //    private String customerId;
    private String status;
    private String userPoolId;
    private Boolean phoneVerified;
    private String unionid;
    private String openid;
    private String identities;
    private String registerSource;
    private String photo;
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


    @Field("listtags")
    private List<Map<String, Object>> listTags;

    private List<Object> roleusers;

    private String source;

    private Integer loginTimes;

    private Date loginTime;

    private Integer role;

    private String validateCode;
    private Date validateCodeSendTime;


}
