package com.tapdata.tm.commons.cdcdelay;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        LongConsumer consumer=(time)->{
        };
        List<String> tables=new ArrayList<>();
        tables.add("testTableId");
        cdcDelay.addHeartbeatTable(tables);
        TapEvent tapEvent = cdcDelay.filterAndCalcDelay(tapUpdateRecordEvent, consumer, TaskDto.SYNC_TYPE_SYNC);
        assertEquals(true,tapEvent instanceof HeartbeatEvent);
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
        LongConsumer consumer=(time)->{
        };
        List<String> tables=new ArrayList<>();
        tables.add("testTableId");
        cdcDelay.addHeartbeatTable(tables);
        TapEvent tapEvent = cdcDelay.filterAndCalcDelay(tapUpdateRecordEvent, consumer, TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        assertEquals(true,tapEvent instanceof TapUpdateRecordEvent);
    }
    @DisplayName("test filterAndCalcDelay not heartbeat event")
    @Test
    void test3(){
        ReflectionTestUtils.setField(cdcDelay,"lastUpdated",System.currentTimeMillis()-5000L*1000);
        TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
        Map<String,Object> after=new HashMap<>();
        after.put("ts",new Date());
        after.put("id","recordId");
        tapUpdateRecordEvent.setReferenceTime(System.currentTimeMillis());
        tapUpdateRecordEvent.setAfter(after);
        tapUpdateRecordEvent.setTableId("testTable");
        LongConsumer consumer=(time)->{
        };
        TapEvent tapEvent = cdcDelay.filterAndCalcDelay(tapUpdateRecordEvent, consumer, TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        assertEquals(tapUpdateRecordEvent,tapEvent);
    }

    @Test
    void testLocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        long l = now.toInstant(ZoneOffset.UTC).toEpochMilli();
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init()
                .after(new Document("id",1).append("ts", now));
        Long aLong = CdcDelay.parseTs(insertRecordEvent);
        Assertions.assertNotNull(aLong);
        Assertions.assertEquals(l, aLong);
    }
    @Test
    void testDate() {
        Date now = new Date();
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init()
                .after(new Document("id",1).append("ts", now));
        Long aLong = CdcDelay.parseTs(insertRecordEvent);
        Assertions.assertNotNull(aLong);
        Assertions.assertEquals(now.getTime(), aLong);
    }
    @Test
    void testInstant() {
        Instant now = new Date().toInstant();
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init()
                .after(new Document("id",1).append("ts", now));
        Long aLong = CdcDelay.parseTs(insertRecordEvent);
        Assertions.assertNotNull(aLong);
        Assertions.assertEquals(now.toEpochMilli(), aLong);
    }
    @Test
    void testLong() {
        Date now = new Date();
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init()
                .after(new Document("id",1).append("ts", now.getTime()));
        Long aLong = CdcDelay.parseTs(insertRecordEvent);
        Assertions.assertNotNull(aLong);
        Assertions.assertEquals(now.getTime(), aLong);
    }
    @Test
    void testString() {
        TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init()
                .after(new Document("id",1).append("ts", "2024-07-25 10:50:46.001")).referenceTime(0L);
        Long aLong = CdcDelay.parseTs(insertRecordEvent);
        Assertions.assertNotNull(aLong);
        Assertions.assertEquals(0L, aLong);
    }
}
