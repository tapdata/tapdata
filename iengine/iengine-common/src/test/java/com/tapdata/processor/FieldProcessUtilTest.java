package com.tapdata.processor;

import com.tapdata.entity.FieldProcess;
import org.junit.Assert;
import org.junit.Test;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class FieldProcessUtilTest {

    /**
     * 测试符合日期格式 yyyy-MM-dd 的字符串转化为Date类型
     * Example input: record:{"LAST_DATE": "2023-11-06", "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     * Expected output: record:{"LAST_DATE": 2023-11-06, "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     */
    @Test
    public void validDateFormatConvertTest() throws ParseException {
        String inputDateString = "2023-11-07";
        Date expectedDate = new SimpleDateFormat("yyyy-MM-dd").parse(inputDateString);
        Map<String, Object> record = new HashMap<>();
        record.put("_id", "651074ca270a1cf5533d162");
        record.put("LAST_DATE", "2023-11-07");
        record.put("CLAIM_ID", "1");
        //Mock 字段操作信息
        FieldProcess fieldProcess = new FieldProcess();
        fieldProcess.setOp(FieldProcess.FieldOp.OP_CONVERT.getOperation());
        fieldProcess.setField("LAST_DATE");
        fieldProcess.setOperand("Date");
        fieldProcess.setOriginedatatype("VARCHAR2(1024)");

        List<FieldProcess> fieldProcesses = Arrays.asList(fieldProcess);
        try {
            FieldProcessUtil.filedProcess(record, fieldProcesses, null, false);
            Date resultDate = (Date) record.get("LAST_DATE");
            Assert.assertNotNull(resultDate);
            Assert.assertEquals(expectedDate, resultDate);
            Assert.assertEquals("651074ca270a1cf5533d162",record.get("_id"));
            Assert.assertEquals("1",record.get("CLAIM_ID"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试不符合格式 yyyy-MM-dd 的字符串格式转化为Date类型
     * Example input: {"value": "DateTime nano 0 seconds 1327149189 timeZone null", "newDateType": "Date"}
     * Expected output: IllegalArgumentException(DateTime constructor illegal dateStr: value)
     */
    @Test(expected = RuntimeException.class)
    public void illegalDateFormatConvertTest() {
        Map<String, Object> record = new HashMap<>();
        record.put("_id", "651074ca270a1cf5533d162");
        record.put("LAST_DATE", "DateTime nano 0 seconds 1327149189 timeZone");
        record.put("CLAIM_ID", "1");
        //Mock 字段操作信息
        FieldProcess fieldProcess = new FieldProcess();
        fieldProcess.setOp(FieldProcess.FieldOp.OP_CONVERT.getOperation());
        fieldProcess.setField("LAST_DATE");
        fieldProcess.setOperand("Date");
        fieldProcess.setOriginedatatype("VARCHAR2(1024)");
        List<FieldProcess> fieldProcesses = Arrays.asList(fieldProcess);
        try {
            FieldProcessUtil.filedProcess(record, fieldProcesses, null, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 测试Long类型转为Date类型
     * Example input:  record: {"LAST_DATE": 1699257206, "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     * Expected output: record: {"LAST_DATE": 2023-11-06, "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     */
    @Test
    public void longTypeConvertTest() {
        Map<String, Object> record = new HashMap<>();
        Long expectedDate = 1699286400L;
        record.put("LAST_DATE", expectedDate);
        record.put("_id", "651074ca270a1cf5533d162");
        record.put("CLAIM_ID", "1");
        //Mock 字段操作信息
        FieldProcess fieldProcess = new FieldProcess();
        fieldProcess.setOp(FieldProcess.FieldOp.OP_CONVERT.getOperation());
        fieldProcess.setField("LAST_DATE");
        fieldProcess.setOperand("Date");
        List<FieldProcess> fieldProcesses = Arrays.asList(fieldProcess);
        try {
            FieldProcessUtil.filedProcess(record, fieldProcesses, null, false);
            Date resultDate = (Date) record.get("LAST_DATE");
            Assert.assertNotNull(resultDate);
            Assert.assertTrue(expectedDate == resultDate.getTime());
            Assert.assertEquals("651074ca270a1cf5533d162", record.get("_id"));
            Assert.assertEquals("1", record.get("CLAIM_ID"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试null值转换为Date类型
     * Example input : record:{"LAST_DATE": null, "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     * Expected output: record:{"LAST_DATE": null, "CLAIM_ID": "1"，"_id"："651074ca270a1cf5533d162"}
     */
    @Test
    public void nullValueConvertTest() {
        Map<String, Object> record = new HashMap<>();
        record.put("_id", "651074ca270a1cf5533d162");
        record.put("LAST_DATE", null);
        record.put("CLAIM_ID", "1");
        //Mock 字段操作信息
        FieldProcess fieldProcess = new FieldProcess();
        fieldProcess.setOp(FieldProcess.FieldOp.OP_CONVERT.getOperation());
        fieldProcess.setField("LAST_DATE");
        fieldProcess.setOperand("Date");
        fieldProcess.setOriginedatatype("VARCHAR2(1024)");
        List<FieldProcess> fieldProcesses = Arrays.asList(fieldProcess);
        try {
            FieldProcessUtil.filedProcess(record, fieldProcesses, null, false);
            Assert.assertNull(record.get("LAST_DATE"));
            Assert.assertEquals("651074ca270a1cf5533d162", record.get("_id"));
            Assert.assertEquals("1", record.get("CLAIM_ID"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
