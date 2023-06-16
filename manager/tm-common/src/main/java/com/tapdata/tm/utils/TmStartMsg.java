package com.tapdata.tm.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TmStartMsg {
    private String status;
    private String msg;

}
