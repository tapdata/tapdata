package com.tapdata.tm.commons.dag.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class FieldInfo implements Serializable {
    private String sourceFieldName;
    private String targetFieldName;
    private Boolean isShow;
    private String type;
}
