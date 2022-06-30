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
@AllArgsConstructor
@NoArgsConstructor
public class TaskUpAndLoadDto {
    private String collectionName;
    private String json;
}
