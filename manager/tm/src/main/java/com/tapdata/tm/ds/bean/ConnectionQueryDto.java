package com.tapdata.tm.ds.bean;

import com.tapdata.tm.ds.constant.ConnectType;
import lombok.*;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/8/20
 * @Description:
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class ConnectionQueryDto extends QueryDto {
    /** 状态  ready invalid */
    private String status;
    /** 查询的分类列表 */
    private List<String> tagList;
    /** 连接类型 源，目标，源&目标 */
    private ConnectType connectType;
    /** 数据源类型id 取数据源定义的id */
    private String DataSourceDefinitionId;
    /** 数据源名称  */
    private String name;
}
