package com.tapdata.tm.openapi.generator.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * OpenAPI Generator configuration properties
 *
 * @author tapdata
 * @date 2024/12/19
 */
@Data
@ConfigurationProperties(prefix = "openapi.generator")
public class OpenApiGeneratorProperties {

    /**
     * JAR file configuration
     */
    private Jar jar = new Jar();

    /**
     * Template configuration
     */
    private Template template = new Template();

    /**
     * Temporary directory configuration
     */
    private Temp temp = new Temp();

    /**
     * Java configuration
     */
    private Java java = new Java();

    @Data
    public static class Jar {
        /**
         * Path to OpenAPI Generator CLI JAR file
         * Auto-detects between development and production paths
         */
        private String path = "auto";
    }

    @Data
    public static class Template {
        /**
         * Path to template directory
         * Auto-detects between development and production paths
         */
        private String path = "auto";
    }

    @Data
    public static class Temp {
        /**
         * Temporary directory for code generation
         */
        private String dir = System.getProperty("java.io.tmpdir");
    }

    @Data
    public static class Java {
        /**
         * Java version for code generation
         */
        private int version = 11;
    }
}
