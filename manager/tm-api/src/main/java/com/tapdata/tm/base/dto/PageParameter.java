package com.tapdata.tm.base.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PageParameter {
    private Integer page;
    private Integer pageSize;
}
