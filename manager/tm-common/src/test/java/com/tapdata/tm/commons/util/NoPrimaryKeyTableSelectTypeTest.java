package com.tapdata.tm.commons.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NoPrimaryKeyTableSelectTypeTest {
    @Test
    void testParse() {
        assertEquals(NoPrimaryKeyTableSelectType.HasKeys, NoPrimaryKeyTableSelectType.parse("HasKeys"));
        assertEquals(NoPrimaryKeyTableSelectType.NoKeys, NoPrimaryKeyTableSelectType.parse("NoKeys"));
        assertEquals(NoPrimaryKeyTableSelectType.All, NoPrimaryKeyTableSelectType.parse("All"));
        assertEquals(NoPrimaryKeyTableSelectType.OnlyPrimaryKey, NoPrimaryKeyTableSelectType.parse("OnlyPrimaryKey"));
        assertEquals(NoPrimaryKeyTableSelectType.OnlyUniqueIndex, NoPrimaryKeyTableSelectType.parse("OnlyUniqueIndex"));

    }
}
