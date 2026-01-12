package com.tapdata.tm.group.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class GroupInfoDto extends BaseDto {
    private String name;
    private String description;
    private List<ResourceItem> resourceItemList = new ArrayList<>();
}
