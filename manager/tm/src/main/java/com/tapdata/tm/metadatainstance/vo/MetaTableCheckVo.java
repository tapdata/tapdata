package com.tapdata.tm.metadatainstance.vo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class MetaTableCheckVo {
    private List<String> exitsTables;
    private List<String> errorTables;
}
