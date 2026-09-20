package com.tapdata.tm.batchtags.service;

import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.task.param.BatchApplyListTagsParam;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchListTagServiceTest {

    private final BatchListTagService service = new BatchListTagService();

    @Test
    void resolveTagChangesShouldIgnoreSomeAndInvalidTags() {
        BatchListTagService.BatchListTagChanges changes = service.resolveTagChanges(Arrays.asList(
                tagState("tag-a", "TagA", "all"),
                tagState("tag-b", "TagB", "none"),
                tagState("tag-c", "TagC", "some"),
                tagState("", "Empty", "all"),
                null
        ));

        assertFalse(changes.isEmpty());
        assertEquals("TagA", changes.getAddTags().get("tag-a").getValue());
        assertTrue(changes.getRemoveTagIds().contains("tag-b"));
        assertFalse(changes.getAddTags().containsKey("tag-c"));
        assertFalse(changes.getRemoveTagIds().contains("tag-c"));
    }

    @Test
    void resolveTagChangesShouldLetLaterDesiredStateWin() {
        BatchListTagService.BatchListTagChanges changes = service.resolveTagChanges(Arrays.asList(
                tagState("tag-a", "TagA", "all"),
                tagState("tag-a", "TagA", "none")
        ));

        assertFalse(changes.getAddTags().containsKey("tag-a"));
        assertTrue(changes.getRemoveTagIds().contains("tag-a"));
    }

    @Test
    void applyDesiredTagsShouldRemoveAndAppendTagsWithoutMutatingSource() {
        List<Tag> currentTags = Arrays.asList(
                new Tag("tag-a", "TagA"),
                new Tag("tag-b", "TagB")
        );
        BatchListTagService.BatchListTagChanges changes = service.resolveTagChanges(Arrays.asList(
                tagState("tag-b", "TagB", "none"),
                tagState("tag-c", "TagC", "all")
        ));

        List<Tag> result = service.applyDesiredTags(currentTags, changes);

        assertEquals(Arrays.asList(new Tag("tag-a", "TagA"), new Tag("tag-c", "TagC")), result);
        assertEquals(Arrays.asList(new Tag("tag-a", "TagA"), new Tag("tag-b", "TagB")), currentTags);
    }

    @Test
    void isSameTagsShouldCompareByIdAndValueIgnoringOrderAndInvalidItems() {
        List<Tag> oldTags = Arrays.asList(
                new Tag("tag-a", "TagA"),
                new Tag("", "Blank"),
                null,
                new Tag("tag-b", "TagB")
        );
        List<Tag> newTags = Arrays.asList(
                new Tag("tag-b", "TagB"),
                new Tag("tag-a", "TagA")
        );

        assertTrue(service.isSameTags(oldTags, newTags));
        assertFalse(service.isSameTags(oldTags, Collections.singletonList(new Tag("tag-a", "TagA"))));
    }

    @Test
    void shouldConvertBetweenTagListAndMapList() {
        List<Map<String, String>> mapTags = service.toMapTags(Arrays.asList(
                new Tag("tag-a", "TagA"),
                new Tag("", "Blank"),
                null
        ));

        assertEquals(1, mapTags.size());
        assertEquals("tag-a", mapTags.get(0).get("id"));
        assertEquals("TagA", mapTags.get(0).get("value"));

        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("id", "tag-b");
        tagMap.put("value", "TagB");
        assertEquals(Collections.singletonList(new Tag("tag-b", "TagB")), service.fromMapTags(Collections.singletonList(tagMap)));
    }

    private BatchApplyListTagsParam.TagState tagState(String id, String value, String desired) {
        BatchApplyListTagsParam.TagState tagState = new BatchApplyListTagsParam.TagState();
        tagState.setId(id);
        tagState.setValue(value);
        tagState.setDesired(desired);
        return tagState;
    }
}
