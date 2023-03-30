package com.tapdata.tm.task.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LdpFuzzySearchVo {
    private FuzzyType type;
    private BaseDto dto;
    private String conId;


    public static enum FuzzyType {
        connection,
        metadata,
        ;
    }
}
