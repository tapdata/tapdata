package com.tapdata.tm.openapi.generator.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI Generator configuration
 * 
 * @author tapdata
 * @date 2024/12/19
 */
@Configuration
@EnableConfigurationProperties(OpenApiGeneratorProperties.class)
public class OpenApiGeneratorConfiguration {
}
