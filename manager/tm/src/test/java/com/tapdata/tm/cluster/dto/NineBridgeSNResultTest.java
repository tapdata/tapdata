package com.tapdata.tm.cluster.dto;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NineBridgeSNResult
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 */
class NineBridgeSNResultTest {
    
    @Nested
    class NineBridgeSNResultMainClassTest {
        
        @Test
        void testGettersAndSetters() {
            NineBridgeSNResult result = new NineBridgeSNResult();
            
            NineBridgeSNResult.DataInfo dataInfo = new NineBridgeSNResult.DataInfo();
            result.setData(dataInfo);
            result.setMessage("Success");
            result.setStatus("OK");
            
            assertEquals(dataInfo, result.getData());
            assertEquals("Success", result.getMessage());
            assertEquals("OK", result.getStatus());
        }
    }
    
    @Nested
    class DataInfoTest {
        
        @Test
        void testGettersAndSetters() {
            NineBridgeSNResult.DataInfo dataInfo = new NineBridgeSNResult.DataInfo();
            
            dataInfo.setDays(365);
            dataInfo.setIssueDate("2024-01-01");
            dataInfo.setModules(10);
            dataInfo.setNHosts(5);
            dataInfo.setOraVersion("12c");
            dataInfo.setPlatForm("Linux");
            dataInfo.setServerId("server123");
            dataInfo.setTimestamp(1234567890L);
            dataInfo.setUser("admin");
            
            assertEquals(365, dataInfo.getDays());
            assertEquals("2024-01-01", dataInfo.getIssueDate());
            assertEquals(10, dataInfo.getModules());
            assertEquals(5, dataInfo.getNHosts());
            assertEquals("12c", dataInfo.getOraVersion());
            assertEquals("Linux", dataInfo.getPlatForm());
            assertEquals("server123", dataInfo.getServerId());
            assertEquals(1234567890L, dataInfo.getTimestamp());
            assertEquals("admin", dataInfo.getUser());
        }
        
        @Test
        void testParseWithValidMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("days", 365);
            map.put("issueDate", "2024-01-01");
            map.put("modules", 10);
            map.put("nHosts", 5);
            map.put("oraVersion", "12c");
            map.put("platForm", "Linux");
            map.put("serverId", "server123");
            map.put("timestamp", 1234567890L);
            map.put("user", "admin");
            
            NineBridgeSNResult.DataInfo dataInfo = NineBridgeSNResult.DataInfo.parse(map);
            
            assertNotNull(dataInfo);
            assertEquals(365, dataInfo.getDays());
            assertEquals("2024-01-01", dataInfo.getIssueDate());
            assertEquals(10, dataInfo.getModules());
            assertEquals(5, dataInfo.getNHosts());
            assertEquals("12c", dataInfo.getOraVersion());
            assertEquals("Linux", dataInfo.getPlatForm());
            assertEquals("server123", dataInfo.getServerId());
            assertEquals(1234567890L, dataInfo.getTimestamp());
            assertEquals("admin", dataInfo.getUser());
        }
        
        @Test
        void testParseWithNullObject() {
            NineBridgeSNResult.DataInfo dataInfo = NineBridgeSNResult.DataInfo.parse(null);
            
            assertNotNull(dataInfo);
            assertNull(dataInfo.getDays());
            assertNull(dataInfo.getModules());
            assertNull(dataInfo.getNHosts());
            assertNull(dataInfo.getTimestamp());
        }
        
        @Test
        void testParseWithNonMapObject() {
            String nonMapObject = "not a map";
            NineBridgeSNResult.DataInfo dataInfo = NineBridgeSNResult.DataInfo.parse(nonMapObject);
            
            assertNotNull(dataInfo);
            assertNull(dataInfo.getDays());
            assertNull(dataInfo.getModules());
            assertNull(dataInfo.getNHosts());
            assertNull(dataInfo.getTimestamp());
        }
        
        @Test
        void testParseWithEmptyMap() {
            Map<String, Object> emptyMap = new HashMap<>();
            NineBridgeSNResult.DataInfo dataInfo = NineBridgeSNResult.DataInfo.parse(emptyMap);
            
            assertNotNull(dataInfo);
            assertNull(dataInfo.getDays());
            assertEquals("null", dataInfo.getIssueDate());
            assertNull(dataInfo.getModules());
            assertNull(dataInfo.getNHosts());
            assertEquals("null", dataInfo.getOraVersion());
            assertEquals("null", dataInfo.getPlatForm());
            assertEquals("null", dataInfo.getServerId());
            assertNull(dataInfo.getTimestamp());
            assertEquals("null", dataInfo.getUser());
        }
        
        @Test
        void testParseWithStringNumbers() {
            Map<String, Object> map = new HashMap<>();
            map.put("days", "365");
            map.put("modules", "10");
            map.put("nHosts", "5");
            map.put("timestamp", "1234567890");
            
            NineBridgeSNResult.DataInfo dataInfo = NineBridgeSNResult.DataInfo.parse(map);
            
            assertNotNull(dataInfo);
            assertEquals(365, dataInfo.getDays());
            assertEquals(10, dataInfo.getModules());
            assertEquals(5, dataInfo.getNHosts());
            assertEquals(1234567890L, dataInfo.getTimestamp());
        }
    }
    
    @Nested
    class ParseIntTest {

        @Test
        void testParseIntWithInteger() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(123);
            assertEquals(123, result);
        }

        @Test
        void testParseIntWithLong() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(123L);
            assertEquals(123, result);
        }

        @Test
        void testParseIntWithDouble() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(123.45);
            assertEquals(123, result);
        }

        @Test
        void testParseIntWithFloat() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(123.45f);
            assertEquals(123, result);
        }

        @Test
        void testParseIntWithValidString() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("123");
            assertEquals(123, result);
        }

        @Test
        void testParseIntWithInvalidString() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("not a number");
            assertNull(result);
        }

        @Test
        void testParseIntWithEmptyString() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("");
            assertNull(result);
        }

        @Test
        void testParseIntWithNull() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(null);
            assertNull(result);
        }

        @Test
        void testParseIntWithNegativeNumber() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(-123);
            assertEquals(-123, result);
        }

        @Test
        void testParseIntWithNegativeString() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("-123");
            assertEquals(-123, result);
        }

        @Test
        void testParseIntWithZero() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(0);
            assertEquals(0, result);
        }

        @Test
        void testParseIntWithStringZero() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("0");
            assertEquals(0, result);
        }

        @Test
        void testParseIntWithDecimalString() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt("123.45");
            assertNull(result);
        }

        @Test
        void testParseIntWithBoolean() {
            Integer result = NineBridgeSNResult.DataInfo.parseInt(true);
            assertNull(result);
        }
    }

    @Nested
    class ParseLongTest {

        @Test
        void testParseLongWithLong() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(1234567890L);
            assertEquals(1234567890L, result);
        }

        @Test
        void testParseLongWithInteger() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(123);
            assertEquals(123L, result);
        }

        @Test
        void testParseLongWithDouble() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(123.45);
            assertEquals(123L, result);
        }

        @Test
        void testParseLongWithFloat() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(123.45f);
            assertEquals(123L, result);
        }

        @Test
        void testParseLongWithValidString() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("1234567890");
            assertEquals(1234567890L, result);
        }

        @Test
        void testParseLongWithInvalidString() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("not a number");
            assertNull(result);
        }

        @Test
        void testParseLongWithEmptyString() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("");
            assertNull(result);
        }

        @Test
        void testParseLongWithNull() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(null);
            assertNull(result);
        }

        @Test
        void testParseLongWithNegativeNumber() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(-1234567890L);
            assertEquals(-1234567890L, result);
        }

        @Test
        void testParseLongWithNegativeString() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("-1234567890");
            assertEquals(-1234567890L, result);
        }

        @Test
        void testParseLongWithZero() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(0L);
            assertEquals(0L, result);
        }

        @Test
        void testParseLongWithStringZero() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("0");
            assertEquals(0L, result);
        }

        @Test
        void testParseLongWithDecimalString() {
            Long result = NineBridgeSNResult.DataInfo.parseLong("123.45");
            assertNull(result);
        }

        @Test
        void testParseLongWithBoolean() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(false);
            assertNull(result);
        }

        @Test
        void testParseLongWithVeryLargeNumber() {
            Long result = NineBridgeSNResult.DataInfo.parseLong(Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, result);
        }

        @Test
        void testParseLongWithVeryLargeString() {
            String maxLong = String.valueOf(Long.MAX_VALUE);
            Long result = NineBridgeSNResult.DataInfo.parseLong(maxLong);
            assertEquals(Long.MAX_VALUE, result);
        }
    }
}


