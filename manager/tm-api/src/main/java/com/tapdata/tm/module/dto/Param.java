package com.tapdata.tm.module.dto;

import com.tapdata.tm.module.enums.ParamTypeEnum;
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
