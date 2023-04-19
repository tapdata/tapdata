package com.tapdata.tm.worker.vo;

import com.mongodb.BasicDBObject;
import lombok.Data;

import java.util.ArrayList;

@Data
public class CalculationEngineVo {
    private String ProcessId;
    private String filter;
    private ArrayList<BasicDBObject> threadLog;
    private int available;
    private boolean manually;
    private int taskAvailable;
}
