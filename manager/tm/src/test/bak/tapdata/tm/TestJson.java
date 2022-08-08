package com.tapdata.tm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Data;
import org.junit.jupiter.api.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/13 下午2:20
 */
public class TestJson {

    @Test
    public void testJackson() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        A a = objectMapper.readValue("{\"a\":\"test\",\"b\":\"ttt\",\"c\":\"s\"}", A.class);

        System.out.println(a);

    }

    @Data
    public static class A {
        private String a;
        private String b;
    }

}
