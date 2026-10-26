package com.tapdata.tm.commons.alarm;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class GidPrefixTest {

    @Test
    void testConstructorIsPrivate() throws Exception {
        Constructor<GidPrefix> constructor = GidPrefix.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        GidPrefix instance = constructor.newInstance();
        assertNotNull(instance);
    }

    @Test
    void testRegex() {
        assertEquals("a^", GidPrefix.regex(null));
        assertEquals("a^", GidPrefix.regex(""));
        assertFalse(Pattern.compile(GidPrefix.regex(null)).matcher("GID001").find());
        assertFalse(Pattern.compile(GidPrefix.regex("")).matcher("").find());
        assertEquals("^" + Pattern.quote("001"), GidPrefix.regex("001"));
        assertEquals("^" + Pattern.quote("group.sub*+"), GidPrefix.regex("group.sub*+"));
    }

    @Test
    void testMatches() {
        // null / empty branches
        assertFalse(GidPrefix.matches(null, "001"));
        assertFalse(GidPrefix.matches("001", null));
        assertFalse(GidPrefix.matches("001", ""));
        assertFalse(GidPrefix.matches(null, null));
        assertFalse(GidPrefix.matches(null, ""));

        // Match success
        assertTrue(GidPrefix.matches("001002", "001"));
        assertTrue(GidPrefix.matches("001", "001"));

        // Match fail
        assertFalse(GidPrefix.matches("002001", "001"));
        assertFalse(GidPrefix.matches("00", "001"));

        // Special regex characters treated literally (startsWith)
        assertTrue(GidPrefix.matches("a.b.c", "a.b"));
        assertFalse(GidPrefix.matches("axb.c", "a.b"));
    }
}
