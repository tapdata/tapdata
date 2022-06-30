package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/12/1
 * @Description: 结构迁移返回实体
 */
@Data
public class StructureMigrateVO {
    private Integer tableNum;
    private Integer successNum;
    private List<TableStatus> tableStatus;


    @Data
    public static class TableStatus {
        private String database;
        /** 连接名 */
        private String connectionName;
        private String table;
        private Integer progress;
        private String status;
    }
}
