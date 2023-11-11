package io.tapdata.io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;

@AspectTaskSession(excludeTypes = "TEST_TARGET")
public class DefaultSampleTask extends SampleTask {
}
