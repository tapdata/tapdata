package com.tapdata.processor.js;

import io.tapdata.file.operation.FileOperationException;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.*;

class FileOperationServiceLoaderTest {
    @Test
    void missingProviderReturnsStableUnavailableCode() {
        ClassLoader emptyLoader = new URLClassLoader(new URL[0], null);
        FileOperationException error = assertThrows(FileOperationException.class,
                () -> FileOperationServiceLoader.load(emptyLoader));
        assertEquals(io.tapdata.file.operation.FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE, error.getCode());
    }
}
