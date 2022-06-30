package com.tapdata.mongo;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RestTemplateOperatorTest {

	public static void main(String[] args) throws JsonProcessingException {
//    List<String> baseURLs = Arrays.asList("http://127.0.0.1:9999", "http://127.0.0.1:8080", "http://localhost:9999");
//    RestTemplateOperator restTemplateOperator = new RestTemplateOperator(baseURLs, 5000);
//
//    String str = "{\"a\": 123}";
//    boolean post = restTemplateOperator.post(str, "/test/hello", new HashMap<>());
//
//    MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter = new MappingJackson2HttpMessageConverter();
//    mappingJackson2HttpMessageConverter.getObjectMapper().writeValueAsString()

		String x = new MappingJackson2HttpMessageConverter().getObjectMapper().writeValueAsString(null);
		System.out.println(x);


	}

}
