package com.tapdata.tm.worker.dto;

import lombok.Data;

import java.util.Date;

@Data
public class WorkerExpireDto {
    private String userId;
    private String shareUser;
    private Date expireTime;
}
