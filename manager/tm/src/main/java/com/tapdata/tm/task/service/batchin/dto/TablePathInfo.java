package com.tapdata.tm.task.service.batchin.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TablePathInfo {
    String database;
    String schema;
    String table;
}
