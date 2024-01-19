package io.tapdata.flow.engine.V2.cleaner;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:19
 **/
public interface ICleaner {
	CleanResult cleanTaskNode(String taskId, String nodeId);
}
