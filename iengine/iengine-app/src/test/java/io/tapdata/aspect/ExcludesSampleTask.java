package io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;

@AspectTaskSession(excludeTypes = {"include1", "exclude2", "TEST_TARGET"})
public class ExcludesSampleTask extends SampleTask {
}
