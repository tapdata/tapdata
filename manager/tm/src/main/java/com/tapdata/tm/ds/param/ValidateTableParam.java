package com.tapdata.tm.ds.param;

import lombok.*;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=false)
public class ValidateTableParam {
    private String connectionId;
    private List<String> tableList;

}
