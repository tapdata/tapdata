package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.trace.ChangeLogData;
import com.tapdata.tm.trace.param.ChangeLogParam;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/27 17:18 Create
 * @description
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ChangeLog extends ChangeLogParam {
    List<Map<String, Object>> logs;
    String msg;

    public static ChangeLog from(ChangeLogParam param, ChangeLogData logs) {
        ChangeLog changeLog = new ChangeLog();
        changeLog.setLogs(Optional.ofNullable(logs).map(ChangeLogData::getLogs).orElse(new ArrayList<>()));
        changeLog.setConnectionId(param.getConnectionId());
        changeLog.setTable(param.getTable());
        changeLog.setEndTime(param.getEndTime());
        changeLog.setStartTime(param.getStartTime());
        changeLog.setQueryConditions(param.getQueryConditions());
        changeLog.setLimit(param.getLimit());
        Long lastKey = Optional.ofNullable(logs).map(ChangeLogData::getLastKey).orElse(param.getLastKey());
        changeLog.setLastKey(lastKey);
        return changeLog;
    }

    public ChangeLog msg(String msg) {
        this.msg = msg;
        return this;
    }
}
