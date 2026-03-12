package com.tapdata.tm.task.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Zed
 * @Date: 2022/3/16
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskUpAndLoadDto {
    private String collectionName;
    private String json;
    /** 二进制数据，用于Excel等文件导出 */
    private byte[] binaryData;

    public TaskUpAndLoadDto(String collectionName, String json) {
        this.collectionName = collectionName;
        this.json = json;
    }

    public TaskUpAndLoadDto(String collectionName, byte[] binaryData) {
        this.collectionName = collectionName;
        this.binaryData = binaryData;
    }
}
