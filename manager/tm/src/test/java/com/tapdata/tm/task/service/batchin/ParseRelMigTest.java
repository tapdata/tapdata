package com.tapdata.tm.task.service.batchin;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class ParseRelMigTest {
    @Test
    void testParams() {
        Assertions.assertEquals("processorThreadNum", ParseRelMig.PROCESSOR_THREAD_NUM);
        Assertions.assertEquals("catalog", ParseRelMig.CATALOG);
        Assertions.assertEquals("elementType", ParseRelMig.ELEMENT_TYPE);
        Assertions.assertEquals("processor", ParseRelMig.PROCESSOR);
        Assertions.assertEquals("rm_id", ParseRelMig.RM_ID);
    }

    @Nested
    class testRedirect{
        ParseParam param;
        MultipartFile multipartFile;
        @BeforeEach
        void init() {
            multipartFile = mock(MultipartFile.class);
            param = new ParseParam()
                    .withUser(mock(UserDetail.class))
                    .withSource("source")
                    .withSink("sink")
                    .withMultipartFile(multipartFile);
        }
        @Test
        void testLowerVersion() throws Exception {
            when(multipartFile.getBytes()).thenReturn("{\"version\":\"1.2.0\"}".getBytes());
            try(MockedStatic<ParseRelMig> prm = Mockito.mockStatic(ParseRelMig.class)) {
                prm.when(() -> ParseRelMig.redirect(param)).thenCallRealMethod();
                ParseRelMigFile redirect = ParseRelMig.redirect(param);
                Assertions.assertNotNull(redirect);
                Assertions.assertEquals(ParseBaseVersionRelMigImpl.class.getName(), redirect.getClass().getName());
            }
        }
        @Test
        void testHeightVersion() throws Exception {
            when(multipartFile.getBytes()).thenReturn("{\"version\":\"1.3.0\"}".getBytes());
            try(MockedStatic<ParseRelMig> prm = Mockito.mockStatic(ParseRelMig.class)) {
                prm.when(() -> ParseRelMig.redirect(param)).thenCallRealMethod();
                ParseRelMigFile redirect = ParseRelMig.redirect(param);
                Assertions.assertNotNull(redirect);
                Assertions.assertEquals(ParseRelMig13OrMoreImpl.class.getName(), redirect.getClass().getName());
            }
        }
        @Test
        void testThrow() throws IOException {
            when(multipartFile.getBytes()).thenAnswer(a -> {
                throw new Exception("failed");
            });
            try(MockedStatic<ParseRelMig> prm = Mockito.mockStatic(ParseRelMig.class)) {
                prm.when(() -> ParseRelMig.redirect(param)).thenCallRealMethod();
                Assertions.assertThrows(BizException.class, () -> ParseRelMig.redirect(param));
            }
        }
    }

}