
package com.tapdata.tm.commons.task.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Status implements Serializable {

    private String id;
    private String name;
    private String message;
    private String status;
}
