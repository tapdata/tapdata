package com.tapdata.tm.task.utils;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collection;
import java.util.Set;

/**
 * 任务「删除态」判定。
 *
 * <p>界面删除任务走 {@code TaskServiceImpl#remove}：先把 name 改成「原名_随机6位」、原名存进 deleteName、
 * 状态置 deleting，等引擎确认后才由 {@code afterRemove} 置 is_deleted=true。所以一条被删除的记录可能停在
 * 两种状态之一，且两种都对用户不可见（任务列表按 status $nin [deleting, delete_failed] 过滤）。
 *
 * <p>导入比对时这两种都必须视为「有变更、需要恢复」，否则误删的资源再导入也找不回来。注意这是删除的特例：
 * 其余运行状态差异（如 stop → running）仍然按「无变化」处理，不参与比对。
 */
public class TaskDeletionState {

    /** 删除流程进行中的状态：is_deleted 尚未置位，但任务已从列表消失 */
    private static final Set<String> DELETING_STATUSES =
            Set.of(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED);

    private TaskDeletionState() {
    }

    public static boolean isDeletingStatus(String status) {
        return status != null && DELETING_STATUSES.contains(status);
    }

    /**
     * 构造「这批 id 里哪些处于删除态」的查询：is_deleted 已置位，或仍停在删除流程中。
     * 只回读 _id，调用方拿 id 集合做判定即可。
     */
    public static Query deletedQuery(Collection<ObjectId> ids) {
        Query query = new Query(new Criteria().andOperator(
                Criteria.where("_id").in(ids),
                new Criteria().orOperator(
                        Criteria.where("is_deleted").is(true),
                        Criteria.where("status").in(DELETING_STATUSES))));
        query.fields().include("_id");
        return query;
    }
}
