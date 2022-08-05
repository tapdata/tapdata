package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.SubTaskDto;

import java.util.*;

/**
 * @author Dexter
 */
public class EngineSampleHandler extends AbstractHandler {
    private static final String TAG_KEY_ENGINE = "engine";
    public EngineSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> engineTags(String engine) {
        Map<String, String> tags = baseTags(SAMPLE_TYPE_ENGINE);
        tags.put(TAG_KEY_ENGINE, engine);

        return tags;
    }
}
