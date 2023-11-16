package io.tapdata.io.tapdata.handler;

import io.tapdata.observable.metric.handler.Constants;
import io.tapdata.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestConstants {
    @Test
    public void testInputINPUT_DDL_TOTAL(){
        testItem("INPUT_DDL_TOTAL", "inputDdlTotal");
    }
    @Test
    public void testInputINPUT_INSERT_TOTAL(){
        testItem("INPUT_INSERT_TOTAL", "inputInsertTotal");
    }
    @Test
    public void testINPUT_UPDATE_TOTAL(){
        testItem("INPUT_UPDATE_TOTAL", "inputUpdateTotal");
    }
    @Test
    public void testINPUT_DELETE_TOTAL(){
        testItem("INPUT_DELETE_TOTAL", "inputDeleteTotal");
    }
    @Test
    public void testINPUT_OTHERS_TOTAL(){
        testItem("INPUT_OTHERS_TOTAL", "inputOthersTotal");
    }
    @Test
    public void testOUTPUT_DDL_TOTAL(){
        testItem("OUTPUT_DDL_TOTAL", "outputDdlTotal");
    }
    @Test
    public void testOUTPUT_INSERT_TOTAL(){
        testItem("OUTPUT_INSERT_TOTAL", "outputInsertTotal");
    }
    @Test
    public void testOUTPUT_UPDATE_TOTAL(){
        testItem("OUTPUT_UPDATE_TOTAL", "outputUpdateTotal");
    }
    @Test
    public void testOUTPUT_DELETE_TOTAL(){
        testItem("OUTPUT_DELETE_TOTAL", "outputDeleteTotal");
    }
    @Test
    public void testOUTPUT_OTHERS_TOTAL(){
        testItem("OUTPUT_OTHERS_TOTAL", "outputOthersTotal");
    }
    @Test
    public void testINPUT_QPS(){
        testItem("INPUT_QPS", "inputQps");
    }
    @Test
    public void testOUTPUT_QPS(){
        testItem("OUTPUT_QPS", "outputQps");
    }
    @Test
    public void testINPUT_SIZE_QPS(){
        testItem("INPUT_SIZE_QPS", "inputSizeQps");
    }
    @Test
    public void testOUTPUT_SIZE_QPS(){
        testItem("OUTPUT_SIZE_QPS", "outputSizeQps");
    }
    @Test
    public void testTIME_COST_AVG(){
        testItem("TIME_COST_AVG", "timeCostAvg");
    }
    @Test
    public void testREPLICATE_LAG(){
        testItem("REPLICATE_LAG", "replicateLag");
    }
    @Test
    public void testCURR_EVENT_TS(){
        testItem("CURR_EVENT_TS", "currentEventTimestamp");
    }
    @Test
    public void testQPS_TYPE(){
        testItem("QPS_TYPE", "qpsType");
    }
    @Test
    public void testQPS_TYPE_MEMORY(){
        testItem("QPS_TYPE_MEMORY", 1);
    }
    @Test
    public void testQPS_TYPE_COUNT(){
        testItem("QPS_TYPE_COUNT", 0);
    }
    @Test
    public void testOUTPUT_SIZE_QPS_MAX(){
        testItem("OUTPUT_SIZE_QPS_MAX", "outputSizeQpsMax");
    }
    @Test
    public void testOUTPUT_SIZE_QPS_AVG(){
        testItem("OUTPUT_SIZE_QPS_AVG", "outputSizeQpsAvg");
    }

    private void testItem(String fieldName, Object expected){
        Object staticField = TestUtil.getStaticField(Constants.class, fieldName);
        Assert.assertNotNull(staticField);
        Assert.assertEquals(expected, staticField);
    }
}
