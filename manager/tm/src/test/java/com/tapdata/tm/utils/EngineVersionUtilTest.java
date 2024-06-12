package com.tapdata.tm.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EngineVersionUtilTest {
    @Test
    @DisplayName("Should return false when version is empty")
    public void testCheckEngineTransformSchemaWithEmptyVersion() {
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema(""), "Expected false for empty version");
    }

    @Test
    @DisplayName("Should return false when version is -")
    public void testCheckEngineTransformSchemaWithSymbol() {
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema("-"), "Expected false for empty version");
    }

    @Test
    @DisplayName("Should return false when version is null")
    public void testCheckEngineTransformSchemaWithNullVersion() {
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema(null), "Expected false for null version");
    }

    @Test
    @DisplayName("Should return false when version is less than 3.8.0")
    public void testCheckEngineTransformSchemaWithLessThan380() {
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema("v3.6.0-test"), "Expected false for version less than 3.8.0");
    }

    @Test
    public void testCheckEngineTransformSchemaVersionError() {
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema("v3.2-224-gb5d94f4d2-1683360843"), "Expected false for version less than 3.8.0");
    }

    @Test
    @DisplayName("Should return true when version is equal to 3.8.0")
    public void testCheckEngineTransformSchemaWithEqualTo380() {
        assertTrue(EngineVersionUtil.checkEngineTransFormSchema("v3.8.0-test"), "Expected true for version 3.8.0");
    }

    @Test
    @DisplayName("Should return true when version is greater than 3.8.0")
    public void testCheckEngineTransformSchemaWithGreaterThan380() {
        assertTrue(EngineVersionUtil.checkEngineTransFormSchema("v3.8.1-test"), "Expected true for version greater than 3.8.0");
    }

    @Test
    @DisplayName("Should return true when version is greater than 4.0.0")
    public void testCheckEngineTransformSchemaWithGreaterThan400() {
        assertTrue(EngineVersionUtil.checkEngineTransFormSchema("v4.0.0-test"), "Expected true for version greater than 3.8.0");
    }

    @Test
    @DisplayName("Should return true when version starts with 3.8.0 but has additional identifiers")
    public void testCheckEngineTransformSchemaWithStartsWith380AdditionalIdentifiers() {
        assertTrue(EngineVersionUtil.checkEngineTransFormSchema("v3.8.0-alpha1"), "Expected true for version starting with 3.8.0 and having additional identifiers");
        assertFalse(EngineVersionUtil.checkEngineTransFormSchema("v3.8.0+build.123"), "Expected true for version 3.8.0 with build metadata");
    }

    @Test
    public void testEngineVersionUtilConstructor() {
        assertThrows(IllegalStateException.class, EngineVersionUtil::new);
    }
}
