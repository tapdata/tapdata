package com.tapdata.tm.Settings.param;

import lombok.Data;

import java.util.List;

@Data
public class EnterpriseUpdateParam {
    private List runNotification;
    private List systemNotification;
    private List agentNotification;

}
