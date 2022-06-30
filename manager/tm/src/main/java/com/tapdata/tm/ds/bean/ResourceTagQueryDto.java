package com.tapdata.tm.ds.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ResourceTagQueryDto extends QueryDto {
    /** 名称，可以模糊查*/
    private String name;
}
