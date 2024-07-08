package com.tapdata.tm.commons.cdcdelay;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;

public class CdcDelayTest {
    private CdcDelay cdcDelay;
    @BeforeEach
    void beforeSetUp(){
        cdcDelay=new CdcDelay();
    }
    @DisplayName("test filterAndCalcDelay sync Task")
    @Test
    void test1(){
        TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
        Map<String,Object> after=new HashMap<>();
        after.put("ts",new Date());
        after.put("id","recordId");
        tapUpdateRecordEvent.setAfter(after);
        tapUpdateRecordEvent.setTableId(ConnHeartbeatUtils.TABLE_NAME);
        Consumer<Long> consumer=(time)->{
        };
        List<String> tables=new ArrayList<>();
        tables.add("testTableId");
        cdcDelay.addHeartbeatTable(tables);
        cdcDelay.filterAndCalcDelay(tapUpdateRecordEvent,consumer, TaskDto.SYNC_TYPE_SYNC);
    }
    @DisplayName("test filterAndCalcDelay LOG_COLLECTOR Task")
    @Test
    void test2(){
        TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
        Map<String,Object> after=new HashMap<>();
        after.put("ts",new Date());
        after.put("id","recordId");
        tapUpdateRecordEvent.setAfter(after);
        tapUpdateRecordEvent.setTableId(ConnHeartbeatUtils.TABLE_NAME);
        Consumer<Long> consumer=(time)->{
        };
        List<String> tables=new ArrayList<>();
        tables.add("testTableId");
        cdcDelay.addHeartbeatTable(tables);
        cdcDelay.filterAndCalcDelay(tapUpdateRecordEvent,consumer, TaskDto.SYNC_TYPE_LOG_COLLECTOR);
    }
}
