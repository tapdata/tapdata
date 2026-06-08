package com.tapdata.tm.taskrebalance.rule;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Milestone;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Service
public class TaskRebalanceRuleService {
    private static final String READ_CDC_EVENT = "READ_CDC_EVENT";

    /**
     * Converts a task into preview data, calculates priority score, and applies every
     * independent movability rule to produce frontend schedulable status and reason.
     */
    public TaskRebalancePreviewVo.TaskPreview evaluate(TaskDto task, Set<String> onlineAgentIds) {
        TaskRebalancePreviewVo.TaskPreview item = toTaskPreview(task);
        applyPriorityScore(task, item);
        item.setSchedulableStatus("OK");
        if (!onlineAgentIds.contains(task.getAgentId())) {
            reject(item, "AGENT_OFFLINE", "Agent offline");
        } else if (!TaskDto.STATUS_RUNNING.equals(task.getStatus())) {
            reject(item, "STATUS_ERROR", "Task is not running");
        } else if (isManuallySpecified(task)) {
            reject(item, "MANUAL_AGENT", "Task has specified agent");
        } else if (!isIncrementalStarted(task)) {
            reject(item, "INCREMENTAL_NOT_STARTED", "Task has not entered incremental stage");
        } else {
            item.setMovable(true);
        }
        return item;
    }

    public int compareMovePriority(TaskRebalancePreviewVo.TaskPreview left, TaskRebalancePreviewVo.TaskPreview right) {
        return Integer.compare(right.getPriorityScore(), left.getPriorityScore());
    }

    public boolean isMovableAtExecution(TaskDto task) {
        return task != null
                && TaskDto.STATUS_RUNNING.equals(task.getStatus())
                && isIncrementalStarted(task)
                && !isManuallySpecified(task);
    }

    private void reject(TaskRebalancePreviewVo.TaskPreview item, String status, String reason) {
        item.setSchedulableStatus(status);
        item.setReason(reason);
        item.setMovable(false);
    }

    private void applyPriorityScore(TaskDto task, TaskRebalancePreviewVo.TaskPreview item) {
        Map<String, Integer> scoreItems = new LinkedHashMap<>();
        int syncTypeScore = syncTypePriority(task.getSyncType()) * 10000;
        int nodeScore = Math.max(0, 1000 - item.getNodeCount()) * 10;
        int startTimeScore = item.getStartTime() == null ? 0 : (int) Math.min(999, Math.max(0, item.getStartTime() / 1000 / 60 % 1000));
        scoreItems.put("syncType", syncTypeScore);
        scoreItems.put("nodeCount", nodeScore);
        scoreItems.put("startTime", startTimeScore);
        item.setPriorityScore(syncTypeScore + nodeScore + startTimeScore);
        item.setPriorityScoreItems(scoreItems);
    }

    private TaskRebalancePreviewVo.TaskPreview toTaskPreview(TaskDto task) {
        TaskRebalancePreviewVo.TaskPreview item = new TaskRebalancePreviewVo.TaskPreview();
        item.setTaskId(task.getId().toHexString());
        item.setTaskName(task.getName());
        item.setType(task.getType());
        item.setSyncType(task.getSyncType());
        item.setStatus(task.getStatus());
        item.setSourceAgentId(task.getAgentId());
        item.setTargetAgentId(task.getAgentId());
        item.setNodeCount(nodeCount(task));
        item.setStartTime(task.getStartTime() == null ? null : task.getStartTime().getTime());
        return item;
    }

    private boolean isManuallySpecified(TaskDto task) {
        if (task.getAccessNodeType() != null && AccessNodeTypeEnum.isManually(task.getAccessNodeType())) {
            return true;
        }
        return CollectionUtils.isNotEmpty(task.getAccessNodeProcessIdList());
    }

    /**
     * Determines whether the task has entered an incremental-safe phase:
     * CDC tasks are accepted directly, while initial+CDC tasks must show CDC milestone or progress.
     */
    private boolean isIncrementalStarted(TaskDto task) {
        if (ParentTaskDto.TYPE_CDC.equals(task.getType())) {
            return true;
        }
        if (!ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(task.getType())) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(task.getMilestones())) {
            for (Milestone milestone : task.getMilestones()) {
                if (READ_CDC_EVENT.equals(milestone.getCode()) && milestone.getStart() != null) {
                    return true;
                }
            }
        }
        return task.getCurrentEventTimestamp() != null && task.getCurrentEventTimestamp() > 0
                || task.getSnapshotDoneAt() != null;
    }

    private int nodeCount(TaskDto task) {
        DAG dag = task.getDag();
        if (dag == null || CollectionUtils.isEmpty(dag.getNodes())) {
            return 0;
        }
        return dag.getNodes().size();
    }

    /**
     * Returns coarse task-type priority used by the preview sort score, with higher
     * operational task types preferred before lower-priority cache or collector tasks.
     */
    private int syncTypePriority(String syncType) {
        if (TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(syncType)) {
            return 3;
        }
        if (TaskDto.SYNC_TYPE_SYNC.equals(syncType) || TaskDto.SYNC_TYPE_MIGRATE.equals(syncType)) {
            return 2;
        }
        if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(syncType) || TaskDto.SYNC_TYPE_MEM_CACHE.equals(syncType)) {
            return 1;
        }
        return 0;
    }
}
