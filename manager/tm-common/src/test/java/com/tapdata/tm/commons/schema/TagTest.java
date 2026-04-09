package com.tapdata.tm.commons.schema;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TagTest {

    @Test
    void testEqualsSameId() {
        Tag tag1 = new Tag("abc", "Value 1");
        Tag tag2 = new Tag("abc", "Value 2");
        assertEquals(tag1, tag2, "Tags with same id should be equal regardless of value");
    }

    @Test
    void testNotEqualsDifferentId() {
        Tag tag1 = new Tag("abc", "Value");
        Tag tag2 = new Tag("def", "Value");
        assertNotEquals(tag1, tag2, "Tags with different id should not be equal");
    }

    @Test
    void testEqualsNull() {
        Tag tag = new Tag("abc", "Value");
        assertNotEquals(null, tag);
    }

    @Test
    void testHashCodeSameId() {
        Tag tag1 = new Tag("abc", "Value 1");
        Tag tag2 = new Tag("abc", "Value 2");
        assertEquals(tag1.hashCode(), tag2.hashCode(), "Tags with same id should have same hashCode");
    }

    @Test
    void testHashSetDeduplication() {
        Set<Tag> set = new HashSet<>();
        set.add(new Tag("abc", "Value 1"));
        set.add(new Tag("abc", "Value 2"));
        set.add(new Tag("def", "Value 3"));
        assertEquals(2, set.size(), "HashSet should deduplicate tags with same id");
    }

    @Test
    void testContainsInList() {
        java.util.List<Tag> list = new java.util.ArrayList<>();
        list.add(new Tag("abc", "Value 1"));
        assertTrue(list.contains(new Tag("abc", "Value 2")), "List.contains should match by id");
    }
}
