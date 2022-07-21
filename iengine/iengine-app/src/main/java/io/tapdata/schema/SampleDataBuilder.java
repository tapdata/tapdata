package io.tapdata.schema;

import java.util.List;
import java.util.Map;

/**
 * sample data generator
 */
public interface SampleDataBuilder {

	List<Map<String, Object>> get(int limit) throws Throwable;

}
