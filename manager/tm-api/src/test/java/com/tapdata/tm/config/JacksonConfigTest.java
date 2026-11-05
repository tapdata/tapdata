package com.tapdata.tm.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.dto.ResponseMessage;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JacksonConfigTest {

	@Test
	void testObjectIdSerializeToHexStringInResponseMessageData() throws Exception {
		JacksonConfig config = new JacksonConfig();
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectId objectId = new ObjectId("66616f30c7a14d46120ef0b1");
		Map<String, Object> data = new HashMap<>();
		data.put("id", objectId);
		ResponseMessage<Map<String, Object>> responseMessage = new ResponseMessage<>();
		responseMessage.setData(data);

		objectMapper.registerModule(config.objectIdJacksonModule());
		String json = objectMapper.writeValueAsString(responseMessage);

		assertTrue(json.contains("\"id\":\"66616f30c7a14d46120ef0b1\""));
	}
}
