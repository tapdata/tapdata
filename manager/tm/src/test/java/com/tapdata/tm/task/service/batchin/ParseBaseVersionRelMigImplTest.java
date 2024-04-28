package com.tapdata.tm.task.service.batchin;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParseBaseVersionRelMigImplTest {
    @Test
    void testNew() {
        Assertions.assertDoesNotThrow(() -> new ParseBaseVersionRelMigImpl(new ParseParam()));
    }
    @Nested
    class ParseBaseVersionRelMigTest {
        ParseBaseVersionRelMigImpl parseBaseVersionRelMig;
        ParseParam parseParam;
        Map<String, String> tasks;
        @BeforeEach
        void init() {
            tasks = new HashMap<>();
            tasks.put("key", "{}");

            parseParam = mock(ParseParam.class);
            when(parseParam.getSource()).thenReturn("source");
            when(parseParam.getSink()).thenReturn("sink");
            when(parseParam.getUser()).thenReturn(mock(UserDetail.class));
            parseBaseVersionRelMig = mock(ParseBaseVersionRelMigImpl.class);
            parseBaseVersionRelMig.param = parseParam;

            when(parseBaseVersionRelMig.doParse(anyString(), anyString(), any(UserDetail.class))).thenReturn(tasks);
            when(parseBaseVersionRelMig.parse()).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                try {
                    List<TaskDto> parse = parseBaseVersionRelMig.parse();
                    Assertions.assertNotNull(parse);
                    Assertions.assertEquals(1, parse.size());
                } catch (Exception e) {
                    throw e;
                }
            });
            verify(parseBaseVersionRelMig).doParse(anyString(), anyString(), any(UserDetail.class));
            verify(parseParam).getSource();
            verify(parseParam).getSink();
            verify(parseParam).getUser();
        }
        @Test
        void testAfterDoParseResultIsNull() {
            when(parseBaseVersionRelMig.doParse(anyString(), anyString(), any(UserDetail.class))).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> {
                try {
                    List<TaskDto> parse = parseBaseVersionRelMig.parse();
                    Assertions.assertEquals(ArrayList.class.getName(), parse.getClass().getName());
                    Assertions.assertNotNull(parse);
                    Assertions.assertEquals(0, parse.size());
                } catch (Exception e) {
                    throw e;
                }
            });
            verify(parseBaseVersionRelMig).doParse(anyString(), anyString(), any(UserDetail.class));
            verify(parseParam).getSource();
            verify(parseParam).getSink();
            verify(parseParam).getUser();
        }
        @Test
        void testThrowExceptionWhenDoParse() {
            when(parseBaseVersionRelMig.doParse(anyString(), anyString(), any(UserDetail.class))).thenAnswer(a -> {
                throw new Exception("failed");
            });
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    List<TaskDto> parse = parseBaseVersionRelMig.parse();
                    Assertions.assertEquals(ArrayList.class.getName(), parse.getClass().getName());
                    Assertions.assertNotNull(parse);
                    Assertions.assertEquals(0, parse.size());
                } catch (BizException e) {
                    Assertions.assertEquals("relMig.parse.failed", e.getErrorCode());
                    throw e;
                }
            });
            verify(parseBaseVersionRelMig).doParse(anyString(), anyString(), any(UserDetail.class));
            verify(parseParam).getSource();
            verify(parseParam).getSink();
            verify(parseParam).getUser();
        }
    }
}