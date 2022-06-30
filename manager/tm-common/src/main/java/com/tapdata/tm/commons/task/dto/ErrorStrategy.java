
package com.tapdata.tm.commons.task.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class ErrorStrategy implements Serializable {

    private String level;

    private String stack;

    private String strategy;

    private String type;
}
