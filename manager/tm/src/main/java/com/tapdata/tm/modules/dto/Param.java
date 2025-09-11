package com.tapdata.tm.modules.dto;

import com.tapdata.tm.modules.constant.ParamTypeEnum;
import lombok.Data;

@Data
public class Param {
    private String name;

    /**
     * @see ParamTypeEnum
     * */
    private String type;

    private String defaultvalue;

    private String description;

    private boolean required;
}
