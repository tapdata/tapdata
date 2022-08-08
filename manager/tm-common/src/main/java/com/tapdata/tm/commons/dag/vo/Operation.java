package com.tapdata.tm.commons.dag.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Operation implements Serializable {
    /**
     * 前缀
     */
    private String prefix;
    /**
     * 后缀
     */
    private String suffix;

    /**
     * Capitalized toUpperCase 转大写 toLowerCase 转小写 ""不变（默认）
     */
    private String capitalized;
}
