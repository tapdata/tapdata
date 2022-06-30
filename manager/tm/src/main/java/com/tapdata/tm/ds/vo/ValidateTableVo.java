package com.tapdata.tm.ds.vo;

import lombok.*;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=false)
public class ValidateTableVo {
    List<String> tableNotExist;

}
