package com.tapdata.tm.commons.task.dto.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/18 14:19 Create
 * @description
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class AlarmContentVariable implements Serializable {
    String name;
    String label;
    String icon;
}
