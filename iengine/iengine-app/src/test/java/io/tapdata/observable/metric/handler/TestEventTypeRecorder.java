package io.tapdata.observable.metric.handler;

import io.tapdata.observable.metric.handler.HandlerUtil;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestEventTypeRecorder {
    HandlerUtil.EventTypeRecorder recorder;
    @BeforeEach
    public void init() {
        recorder = new HandlerUtil.EventTypeRecorder();
    }

    @Test
    public void testDefaultMemoryUtil() {
        Assert.assertNotNull(recorder.getMemoryUtil());
        Assert.assertEquals("B", recorder.getMemoryUtil());
    }

    @Test
    public void testIncrDdlTotal() {
        recorder.incrDdlTotal();
        Assert.assertEquals(1, recorder.getDdlTotal());
    }
    @Test
    public void testIncrInsertTotal() {
        recorder.incrInsertTotal();
        Assert.assertEquals(1, recorder.getInsertTotal());
    }
    @Test
    public void testIncrUpdateTotal() {
        recorder.incrUpdateTotal();
        Assert.assertEquals(1, recorder.getUpdateTotal());
    }
    @Test
    public void testIncrDeleteTotal() {
        recorder.incrDeleteTotal();
        Assert.assertEquals(1, recorder.getDeleteTotal());
    }
    @Test
    public void testIcrOthersTotal() {
        recorder.incrOthersTotal();
        Assert.assertEquals(1, recorder.getOthersTotal());
    }
    @Test
    public void testGetTotal() {
        recorder.incrDdlTotal();
        long all = recorder.getUpdateTotal() + recorder.getDeleteTotal() + recorder.getDdlTotal() + recorder.getInsertTotal() + recorder.getOthersTotal();
        Assert.assertEquals(all, recorder.getTotal());
    }

    @Test
    public void testIncrProcessTimeTotal() {
        Long now = System.currentTimeMillis();
        Long time = System.currentTimeMillis() - 10;
        recorder.incrProcessTimeTotal(now, time);
        Assert.assertNotNull(recorder.getProcessTimeTotal());
        Assert.assertEquals(new Long(now-time), recorder.getProcessTimeTotal());
    }
    @Test
    public void testIncrProcessTimeTotal0() {
        recorder.incrProcessTimeTotal(System.currentTimeMillis(), null);
        Assert.assertNull(recorder.getProcessTimeTotal());
    }

    @Test
    public void testCalculateMaxReplicateLag(){
        Long time = System.currentTimeMillis();
        List<Long> list = new ArrayList<>();
        list.add(System.currentTimeMillis() - 10);
        recorder.calculateMaxReplicateLag(time, list);
        Assert.assertNotNull(recorder.getReplicateLagTotal());
        Assert.assertEquals(new Long(10), recorder.getReplicateLagTotal());
    }
    @Test
    public void testCalculateMaxReplicateLag0(){
        Long time = System.currentTimeMillis();
        List<Long> list = null;
        recorder.calculateMaxReplicateLag(time, list);
        Assert.assertNull(recorder.getReplicateLagTotal());
    }
    @Test
    public void testCalculateMaxReplicateLag1(){
        Long time = System.currentTimeMillis();
        List<Long> list = new ArrayList<>();
        recorder.calculateMaxReplicateLag(time, list);
        Assert.assertNull(recorder.getReplicateLagTotal());
    }
    @Test
    public void testCalculateMaxReplicateLag2(){
        Long time = System.currentTimeMillis();
        List<Long> list = new ArrayList<>();
        list.add(time - 10);
        list.add(null);
        list.add(time -1);
        recorder.calculateMaxReplicateLag(time, list);
        Assert.assertNotNull(recorder.getReplicateLagTotal());
        Assert.assertEquals(new Long(10), recorder.getReplicateLagTotal());
    }
}
