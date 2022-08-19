package com.tapdata.tm.commons.dag.vo;

import lombok.*;

import java.io.Serializable;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Setter
@ToString
public class FieldInfo implements Serializable {
    private String sourceFieldName;
    private String targetFieldName;
    private Boolean isShow;
    private String type;
}
