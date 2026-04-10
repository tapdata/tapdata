package com.tapdata.tm.vo;

import lombok.*;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

@Data
@EqualsAndHashCode
public class BaseVo implements Serializable {
    private String id;
    private String customId;
    private Date createAt;
    private Date lastUpdAt;
    private String userId;
    private String lastUpdBy;
    private String createUser;
    private Set<String> permissionActions;
}
