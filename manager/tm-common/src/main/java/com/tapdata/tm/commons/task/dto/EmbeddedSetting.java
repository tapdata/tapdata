
package com.tapdata.tm.commons.task.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class EmbeddedSetting implements Serializable {
    /** 选择内嵌的数据对象（仅可以是外键侧的数据表） */
    private List<Object> fields;
    /** 内嵌路径设置（默认为表名） */
    private String format;
    /** 打开之后作为文档还是数组的单选项 */
    private String path;
}
