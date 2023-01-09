package com.tapdata.tm.user.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.user.entity.GuideData;
import com.tapdata.tm.user.entity.Notification;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/11 下午8:05
 * @description
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class UserDto extends BaseDto {

    private String accessCode;
    private String username;
    private String email;
    private Integer role;
    private String phone;
    private String photo;
    private boolean emailVerified;
    @JsonProperty("emailVerified_from_frontend")
    private Boolean emailVerifiedFromFrontend;
    private String externalUserId; // authing or eCloud user id
//    private String userId;

    private String isPrimary;
    private boolean isCompleteGuide;
    @JsonProperty("account_status")
    private int accountStatus;


    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date last_updated;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date createTime;

    private Notification notification;
    private GuideData guideData;
    private List<RoleMappingDto> roleMappings;

    @JsonProperty("listtags")
    private List<Map<String, Object>> listTags;

    private List<Object> roleusers;

    private List<PermissionDto> permissions;

}
