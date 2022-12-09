package com.tapdata.tm.metadatainstance.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DataType2TapTypeDto {
    private String databaseType;
    private Set<String> dataTypes;
}
