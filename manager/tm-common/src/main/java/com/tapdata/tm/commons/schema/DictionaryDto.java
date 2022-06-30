package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;



@EqualsAndHashCode(callSuper = true)
@Data
public class DictionaryDto extends BaseDto {
    private String name;
    private String key;
    private String value;

}