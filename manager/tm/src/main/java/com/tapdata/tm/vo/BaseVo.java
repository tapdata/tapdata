package com.tapdata.tm.vo;

import lombok.*;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper=false)
public class BaseVo {
    private String id;
    private String customId;
    private Date createAt;
    private Date lastUpdAt;
    private String userId;
    private String lastUpdBy;
    private String createUser;
}
