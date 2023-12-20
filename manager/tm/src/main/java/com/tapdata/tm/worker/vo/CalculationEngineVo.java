package com.tapdata.tm.worker.vo;

import com.mongodb.BasicDBObject;
import com.tapdata.tm.worker.dto.WorkSchedule;
import lombok.Data;

import java.util.ArrayList;

@Data
public class CalculationEngineVo {
    private String ProcessId;
    private String filter;
    private ArrayList<WorkSchedule> threadLog;
    private int available;
    private boolean manually;
    private int taskLimit;
    private int runningNum;
    private int totalLimit;
    private int totalRunningNum;
}
