package com.tapdata.tm.commons.dag.vo;

import lombok.Data;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/3/19 下午2:17
 */
@Data
public class CustomTypeMapping {
    private String sourceType;
    private String targetType;
    private Integer precision;     // 数字类型时，设置类型精度
    private Integer length;      // 字段长度，
}
