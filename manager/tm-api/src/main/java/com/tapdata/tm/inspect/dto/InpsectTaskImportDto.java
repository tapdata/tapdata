package com.tapdata.tm.inspect.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InpsectTaskImportDto {
    private String collection;
    private List<InspectDto> data;
}
