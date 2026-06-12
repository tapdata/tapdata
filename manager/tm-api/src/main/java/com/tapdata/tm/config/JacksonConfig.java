package com.tapdata.tm.config;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import org.bson.types.ObjectId;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

	@Bean
	public SimpleModule objectIdJacksonModule() {
		SimpleModule module = new SimpleModule();
		module.addSerializer(ObjectId.class, new ObjectIdSerialize());
		return module;
	}
}
