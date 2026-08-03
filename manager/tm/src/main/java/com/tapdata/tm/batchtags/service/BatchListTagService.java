package com.tapdata.tm.batchtags.service;

import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.task.param.BatchApplyListTagsParam;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class BatchListTagService {

    public BatchListTagChanges resolveTagChanges(List<BatchApplyListTagsParam.TagState> desiredTags) {
        BatchListTagChanges tagChanges = new BatchListTagChanges();
        if (CollectionUtils.isEmpty(desiredTags)) {
            return tagChanges;
        }

        desiredTags.forEach(tag -> {
            if (tag == null || StringUtils.isBlank(tag.getId())) {
                return;
            }

            String desired = StringUtils.lowerCase(tag.getDesired());
            if ("all".equals(desired)) {
                tagChanges.removeTagIds.remove(tag.getId());
                tagChanges.addTags.put(tag.getId(), new Tag(tag.getId(), tag.getValue()));
            } else if ("none".equals(desired)) {
                tagChanges.addTags.remove(tag.getId());
                tagChanges.removeTagIds.add(tag.getId());
            }
        });

        return tagChanges;
    }

    public List<Tag> applyDesiredTags(List<Tag> currentTags, BatchListTagChanges tagChanges) {
        if (tagChanges == null || tagChanges.isEmpty()) {
            return normalizeTags(currentTags);
        }

        Map<String, Tag> tagMap = new LinkedHashMap<>();
        normalizeTags(currentTags).forEach(tag -> tagMap.put(tag.getId(), tag));

        tagChanges.getRemoveTagIds().forEach(tagMap::remove);
        tagChanges.getAddTags().forEach((id, tag) -> tagMap.put(id, new Tag(tag.getId(), tag.getValue())));

        return new ArrayList<>(tagMap.values());
    }

    public boolean isSameTags(List<Tag> oldTags, List<Tag> newTags) {
        return toTagValueMap(oldTags).equals(toTagValueMap(newTags));
    }

    public List<Tag> normalizeTags(List<Tag> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyList();
        }
        return tags.stream()
                .filter(tag -> tag != null && StringUtils.isNotBlank(tag.getId()))
                .map(tag -> new Tag(tag.getId(), tag.getValue()))
                .collect(Collectors.toList());
    }

    public List<Map<String, String>> toMapTags(List<Tag> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyList();
        }
        return tags.stream()
                .filter(tag -> tag != null && StringUtils.isNotBlank(tag.getId()))
                .map(tag -> {
                    Map<String, String> tagMap = new LinkedHashMap<>(2);
                    tagMap.put("id", tag.getId());
                    tagMap.put("value", tag.getValue());
                    return tagMap;
                })
                .collect(Collectors.toList());
    }

    public List<Tag> fromMapTags(List<Map<String, String>> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyList();
        }
        return tags.stream()
                .filter(tag -> tag != null && StringUtils.isNotBlank(tag.get("id")))
                .map(tag -> new Tag(tag.get("id"), tag.get("value")))
                .collect(Collectors.toList());
    }

    private Map<String, String> toTagValueMap(List<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList()).stream()
                .filter(tag -> tag != null && StringUtils.isNotBlank(tag.getId()))
                .collect(Collectors.toMap(Tag::getId, Tag::getValue, (oldValue, newValue) -> newValue, LinkedHashMap::new));
    }

    @Getter
    public static class BatchListTagChanges {
        private final Map<String, Tag> addTags = new LinkedHashMap<>();
        private final Set<String> removeTagIds = new LinkedHashSet<>();

        public boolean isEmpty() {
            return addTags.isEmpty() && removeTagIds.isEmpty();
        }
    }
}
