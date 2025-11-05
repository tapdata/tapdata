package com.tapdata.tm.taskinspect.vo;

import com.tapdata.tm.taskinspect.dto.TaskInspectResultsOpDto;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 增量校验-手动操作交互参数
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/10/30 10:56 Create
 */
@Setter
@Getter
public class ManualOperationVo {

    public static final int STEP_NORMAL = 0;
    public static final int STEP_BEGIN = 1;
    public static final int STEP_END = 2;
    public static final int STEP_BETWEEN = 3;

    private String taskId;
    private String userId;
    private String userName;
    private String manualId;
    private List<String> rowIds; // 可以为 empty，解决遍历结束后没有剩余数据时，发送结束信号
    private int step;

    public boolean isBetween() {
        return STEP_BETWEEN == step;
    }

    public boolean isBegin() {
        return switch (step) {
            case STEP_BEGIN, STEP_BETWEEN -> true;
            default -> false;
        };
    }

    public boolean isEnd() {
        return switch (step) {
            case STEP_END, STEP_BETWEEN -> true;
            default -> false;
        };
    }

    public static ManualOperationVo of(String taskId, String userId, String userName, String manualId, List<String> rowIds) {
        // 所有参数不能为空
        assert taskId != null;
        assert userId != null;
        assert userName != null;
        assert manualId != null;
        assert rowIds != null;

        ManualOperationVo vo = new ManualOperationVo();
        vo.setTaskId(taskId);
        vo.setUserId(userId);
        vo.setUserName(userName);
        vo.setManualId(manualId);
        vo.setRowIds(rowIds);
        vo.setStep(STEP_BETWEEN);
        return vo;
    }

    public static boolean isOperation(String type) {
        return TaskInspectResultsOpDto.OP_MANUAL_CHECK.equals(type)
            || TaskInspectResultsOpDto.OP_MANUAL_RECOVER.equals(type)
            || TaskInspectResultsOpDto.OP_EXPORT_RECOVER_SQL.equals(type)
            ;
    }
}
