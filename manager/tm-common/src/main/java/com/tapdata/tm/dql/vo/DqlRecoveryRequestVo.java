package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class DqlRecoveryRequestVo implements Serializable {
    private List<String> eventIds;
    private Boolean confirm;
}
