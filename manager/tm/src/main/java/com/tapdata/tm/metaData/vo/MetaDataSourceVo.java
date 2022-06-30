
package com.tapdata.tm.metaData.vo;

import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * 数据源连接
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class MetaDataSourceVo extends BaseVo {
    private String customId;
    private String createTime;
    private Date last_updated;
    private String user_id;
    private String lastUpdBy;
    private String createUser;
    /** 数据源连接名称 */
    private String name;
}
