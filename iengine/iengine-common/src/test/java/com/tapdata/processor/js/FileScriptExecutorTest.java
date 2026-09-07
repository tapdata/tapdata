package com.tapdata.processor.js;

import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationResult;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileVerifyMode;
import io.tapdata.file.operation.TapFileOperationService;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FileScriptExecutorTest {

    @Test
    void copyMapsOnlySafeResultAndBuildsExplicitEndpoints() {
        TapFileOperationService service = mock(TapFileOperationService.class);
        when(service.copy(any(FileCopyRequest.class))).thenReturn(FileOperationResult.builder()
                .status(FileOperationStatus.COPIED).sourcePath("a.txt").targetPath("b.txt")
                .bytes(12).attempts(1).durationMs(4).build());
        FileScriptExecutor executor = new FileScriptExecutor(service,
                new DefaultJsNodeConfigAccessor(Collections.emptyList()));

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("source", endpoint("ftp", "/in"));
        request.put("target", endpoint("ftp", "/out"));
        request.put("sourcePath", "a.txt");
        request.put("targetPath", "b.txt");
        request.put("verify", "SIZE");
        request.put("timeoutMs", 3000L);
        Map<String, Object> result = executor.copy(request);

        assertEquals("COPIED", result.get("status"));
        assertEquals(12L, result.get("bytes"));
        verify(service).copy(any(FileCopyRequest.class));
        assertFalse(result.containsKey("password"));
    }

    @Test
    void copyByConfigResolvesPrefixInsideJavaFacade() {
        TapFileOperationService service = mock(TapFileOperationService.class);
        when(service.copy(any(FileCopyRequest.class))).thenReturn(FileOperationResult.builder()
                .status(FileOperationStatus.REUSED).sourcePath("a.txt").targetPath("b.txt")
                .bytes(12).attempts(1).durationMs(4).build());
        DefaultJsNodeConfigAccessor config = new DefaultJsNodeConfigAccessor(Arrays.asList(
                param("mgm.in.protocol", "ftp"), param("mgm.in.host", "in.example"),
                param("mgm.in.rootPath", "/in"), param("mgm.out.protocol", "ftp"),
                param("mgm.out.host", "out.example"), param("mgm.out.rootPath", "/out")));
        FileScriptExecutor executor = new FileScriptExecutor(service, config);

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sourcePrefix", "mgm.in");
        request.put("targetPrefix", "mgm.out");
        request.put("sourcePath", "a.txt");
        request.put("targetPath", "b.txt");
        Map<String, Object> result = executor.copyByConfig(request);

        assertEquals("REUSED", result.get("status"));
        verify(service).copy(any(FileCopyRequest.class));
    }

    private static Map<String, Object> endpoint(String protocol, String rootPath) {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        endpoint.put("protocol", protocol);
        endpoint.put("rootPath", rootPath);
        endpoint.put("params", Collections.singletonMap("host", "example"));
        return endpoint;
    }

    private static JsNodeConfigParam param(String key, String value) {
        return JsNodeConfigParam.builder().key(key).type(JsNodeConfigValueType.STRING).value(value).build();
    }
}
