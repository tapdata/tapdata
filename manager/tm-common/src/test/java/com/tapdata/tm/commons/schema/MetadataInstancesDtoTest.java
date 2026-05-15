package com.tapdata.tm.commons.schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetadataInstancesDtoTest {

    @Test
    void testDeduplicateListtagsRemovesDuplicates() {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        List<Tag> tags = new ArrayList<>(Arrays.asList(
                new Tag("1", "Tag A"),
                new Tag("1", "Tag A"),
                new Tag("2", "Tag B"),
                new Tag("2", "Tag B"),
                new Tag("3", "Tag C")
        ));
        dto.setListtags(tags);

        dto.deduplicateListtags();

        assertEquals(3, dto.getListtags().size(), "Should have 3 unique tags after deduplication");
    }

    @Test
    void testDeduplicateListtagsWithNull() {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        dto.setListtags(null);
        // Should not throw
        dto.deduplicateListtags();
        assertNull(dto.getListtags());
    }

    @Test
    void testDeduplicateListtagsWithEmpty() {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        dto.setListtags(new ArrayList<>());
        dto.deduplicateListtags();
        assertTrue(dto.getListtags().isEmpty());
    }

    @Test
    void testDeduplicateListtagsFiltersNullEntries() {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        List<Tag> tags = new ArrayList<>(Arrays.asList(
                new Tag("1", "Tag A"),
                null,
                new Tag("1", "Tag A duplicate")
        ));
        dto.setListtags(tags);

        dto.deduplicateListtags();

        assertEquals(1, dto.getListtags().size());
        assertNotNull(dto.getListtags().get(0));
    }

    @Test
    void testDeduplicateListtagsKeepsFirst() {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        List<Tag> tags = new ArrayList<>(Arrays.asList(
                new Tag("1", "First"),
                new Tag("1", "Second")
        ));
        dto.setListtags(tags);

        dto.deduplicateListtags();

        assertEquals(1, dto.getListtags().size());
        assertEquals("First", dto.getListtags().get(0).getValue(), "Should keep first occurrence");
    }
}
