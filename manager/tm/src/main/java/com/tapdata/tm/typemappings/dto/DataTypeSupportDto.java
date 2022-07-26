package com.tapdata.tm.typemappings.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午2:12
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class DataTypeSupportDto extends BaseDto {
    private String sourceDbType;
    private List<String> targetDbType;
    private String operator;
    private Object expression;
    private boolean support;
}
