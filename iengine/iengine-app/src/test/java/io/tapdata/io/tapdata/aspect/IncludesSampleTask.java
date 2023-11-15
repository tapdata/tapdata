package io.tapdata.io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;

@AspectTaskSession(includeTypes = {"include1", "include2"})
public class IncludesSampleTask extends SampleTask {
}
