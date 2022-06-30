package com.tapdata.tm.userLog.param;

import lombok.Data;

@Data
public class AddUserLogParam {
    String modular;
    String operation;
    String parameter1;
    String parameter2;
    String parameter3;
    String sourceId;
    Boolean rename;

}
