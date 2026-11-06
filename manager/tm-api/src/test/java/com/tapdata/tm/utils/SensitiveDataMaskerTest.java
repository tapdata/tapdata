package com.tapdata.tm.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SensitiveDataMaskerTest {
    @Test
    @DisplayName("审计脱敏：大小写、重复参数和编码参数名均隐藏值")
    void should_mask_query_values() {
        assertEquals("/mcp?AccessCode=***&token=***&token=***&access%43ode=***",
                SensitiveDataMasker.mask("/mcp?AccessCode=first&token=second&token=third&access%43ode=fourth"));
    }

    @Test
    @DisplayName("审计脱敏：JSON、空值、Bearer和URL密码")
    void should_mask_credential_formats() {
        assertEquals("{\"password\":\"***\",\"name\":\"safe\"}",
                SensitiveDataMasker.mask("{\"password\":\"synthetic value\",\"name\":\"safe\"}"));
        assertEquals("{\"password\":\"***\"}",
                SensitiveDataMasker.mask("{\"password\":\"synthetic \\\" quoted value\"}"));
        assertEquals("Bearer ***", SensitiveDataMasker.mask("Bearer synthetic.value"));
        assertEquals("https://user:***@example.invalid/path", SensitiveDataMasker.mask("https://user:synthetic@example.invalid/path"));
        assertEquals(null, SensitiveDataMasker.mask(null));
        assertEquals("", SensitiveDataMasker.mask(""));
    }

    @Test
    @DisplayName("审计脱敏：重复运行幂等且普通文本保持原样")
    void should_preserve_safe_audit() {
        String text = "Created connection orders";
        assertEquals(text, SensitiveDataMasker.mask(text));
        String masked = SensitiveDataMasker.mask("/mcp?accessCode=synthetic");
        assertEquals(masked, SensitiveDataMasker.mask(masked));
    }
}
