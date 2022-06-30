package com.tapdata.tm.libSupported.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * LibSupporteds
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LibSupportedsDto extends BaseDto {

    private String databaseType;

    private Object supportedList;

}