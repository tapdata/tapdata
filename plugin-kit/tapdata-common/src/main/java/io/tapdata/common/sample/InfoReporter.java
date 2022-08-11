package io.tapdata.common.sample;

import java.util.Map;

/**
 *
 * @author dxtr
 */
public interface InfoReporter {
    /**
     *
     */
    void execute(Map<String, Object> pointValues, Map<String, String> tags);
}
