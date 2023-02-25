package io.tapdata.flow.engine.V2.task.cleaner;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 16:53
 **/
public enum NodeResetDesc {
	task_reset_start,
	task_reset_end,
	task_reset_pdk_node_external_resource,
	task_reset_pdk_node_state,
	task_reset_merge_node,
	task_reset_aggregate_node,
	task_reset_custom_node,
	task_reset_join_node,
	unknown_error,
}
