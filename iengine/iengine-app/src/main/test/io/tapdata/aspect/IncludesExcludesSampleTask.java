package io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;

@AspectTaskSession(includeTypes = {"include1", "include2"}, excludeTypes = {"include1", "exclude2"})
public class IncludesExcludesSampleTask extends SampleTask {
}
