package com.tapdata.tm.ds.bean;

import lombok.*;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataSourceDefinitionQueryDto extends QueryDto {
    /** 名称，可以模糊查*/
    private String dataSourceType;

    private String typeId;
}
