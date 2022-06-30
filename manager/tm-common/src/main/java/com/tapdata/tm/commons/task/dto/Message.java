package com.tapdata.tm.commons.task.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message implements Serializable {
    private String code;
    private String msg;
    private String msgStack;
    private Object data;
}
