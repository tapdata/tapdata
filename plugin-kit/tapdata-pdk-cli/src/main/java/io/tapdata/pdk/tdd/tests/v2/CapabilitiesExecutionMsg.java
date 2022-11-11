package io.tapdata.pdk.tdd.tests.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CapabilitiesExecutionMsg {
    public static final int ERROR = 0;
    public static final int SUCCEED = 1;
    int executionTimes = 0;
    String executionMsg;
    List<History> executionHistory = new ArrayList<>();
    int executionResult = SUCCEED;

    public static CapabilitiesExecutionMsg create(){
        return new CapabilitiesExecutionMsg();
    }
    public CapabilitiesExecutionMsg addTimes(){
        this.executionTimes++;
        return this;
    }
    public CapabilitiesExecutionMsg addTimes(int executionTimes){
        this.executionTimes += executionTimes;
        return this;
    }
    public CapabilitiesExecutionMsg executionMsg(String executionMsg){
        this.executionMsg = executionMsg;
        return this;
    }
    public CapabilitiesExecutionMsg addHistory(History history){
        if (null!=history) {
            this.executionHistory.add(history);
        }
        return this;
    }
    public CapabilitiesExecutionMsg clean(){
        this.executionTimes = 0;
        this.executionHistory = new ArrayList<>();
        this.executionMsg = "";
        return this;
    }
    public CapabilitiesExecutionMsg fail(){
        this.executionResult = ERROR;
        return this;
    }

    public int executionResult(){
        return this.executionResult;
    }
    public List<History> history(){
        return this.executionHistory;
    }
    public int executionTimes(){
        return this.executionTimes;
    }


}
