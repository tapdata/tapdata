package com.tapdata.tm.log.service;

import com.tapdata.tm.log.dto.LogDto;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class LogServiceTest {
    LogService logService = new LogService();

    @Nested
    class ProcessLogDtoTest {

        @Test
        void testProcessLogDto_WithNullLogDto() {
            LogDto logDto = null;
            assertDoesNotThrow(() -> logService.processLogDto(logDto));
        }

        @Test
        void testProcessLogDto_WithNullDate() {
            LogDto logDto = new LogDto();
            logDto.setDate(null);
            logService.processLogDto(logDto);
            assertNull(logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithLongTimestamp() {
            LogDto logDto = new LogDto();
            long timestamp = 1697123456789L;
            logDto.setDate(timestamp);
            logService.processLogDto(logDto);
            assertNotNull(logDto.getDate());
            assertInstanceOf(Date.class, logDto.getDate());
            assertEquals(new Date(timestamp), logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithIntegerTimestamp() {
            LogDto logDto = new LogDto();
            int timestamp = 1697123456;
            logDto.setDate(timestamp);
            logService.processLogDto(logDto);
            assertNotNull(logDto.getDate());
            assertInstanceOf(Date.class, logDto.getDate());
            assertEquals(new Date(timestamp), logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithDoubleTimestamp() {
            LogDto logDto = new LogDto();
            double timestamp = 1697123456789.0;
            logDto.setDate(timestamp);
            logService.processLogDto(logDto);
            assertNotNull(logDto.getDate());
            assertInstanceOf(Date.class, logDto.getDate());
            assertEquals(new Date((long) timestamp), logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithStringTimestamp() {
            LogDto logDto = new LogDto();
            String timestamp = "1697123456789";
            logDto.setDate(timestamp);
            logService.processLogDto(logDto);
            assertNotNull(logDto.getDate());
            assertInstanceOf(Date.class, logDto.getDate());
            assertEquals(new Date(Long.parseLong(timestamp)), logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithNonNumericString() {
            LogDto logDto = new LogDto();
            String nonNumericString = "2023-10-13T10:30:45Z";
            logDto.setDate(nonNumericString);
            logService.processLogDto(logDto);
            assertEquals(nonNumericString, logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithExistingDateObject() {
            LogDto logDto = new LogDto();
            Date existingDate = new Date();
            logDto.setDate(existingDate);
            logService.processLogDto(logDto);
            assertSame(existingDate, logDto.getDate());
            assertInstanceOf(Date.class, logDto.getDate());
        }

        @Test
        void testProcessLogDto_WithUnexpectedType() {
            LogDto logDto = new LogDto();
            Boolean unexpectedType = Boolean.TRUE;
            logDto.setDate(unexpectedType);
            logService.processLogDto(logDto);
            assertEquals(unexpectedType, logDto.getDate());
        }
    }
}
