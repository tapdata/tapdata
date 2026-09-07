package com.tapdata.tm.commons.dag.vo;

import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import com.tapdata.tm.commons.dag.process.script.JsNodeConfigValueType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class TestRunDtoTest {
    @Test
    void carriesScriptParamsForUnsavedTestRunValues() {
        TestRunDto dto = new TestRunDto();
        dto.setScriptParams(Collections.singletonList(JsNodeConfigParam.builder()
                .key("mgm.in.host").type(JsNodeConfigValueType.STRING).value("ftp.example").build()));
        assertEquals("mgm.in.host", dto.getScriptParams().get(0).getKey());
    }
}
