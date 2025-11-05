package com.tapdata.tm.taskinspect.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * 任务内校验-差异操作记录
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 21:58 Create
 */
@Data
public class ResultOperationsVo implements Serializable {

    /**
     * 增量校验
     */
    public static final String OP_CDC_CHECK = "CDC_CHECK";
    /**
     * 自动二次校验
     */
    public static final String OP_AUTO_RECHECK = "AUTO_RECHECK";
    /**
     * 自动修复
     */
    public static final String OP_AUTO_RECOVER = "AUTO_RECOVER";
    /**
     * 自动修复校验
     */
    public static final String OP_AUTO_RECOVER_CHECK = "AUTO_RECOVER_CHECK";
    /**
     * 手动校验
     */
    public static final String OP_MANUAL_CHECK = "MANUAL_CHECK";
    /**
     * 手动修复
     */
    public static final String OP_MANUAL_RECOVER = "MANUAL_RECOVER";
    /**
     * 手动修复校验
     */
    public static final String OP_MANUAL_RECOVER_CHECK = "MANUAL_RECOVER_CHECK";
    /**
     * 导出修复 SQL
     */
    public static final String OP_EXPORT_RECOVER_SQL = "EXPORT_RECOVER_SQL";

    private Long ts; // 操作时间
    private String op; // 操作类型
    private String msg; // 操作信息
    private String userId; // 用户编号
    private String user; // 用户名

    /**
     * 创建一个表示操作结果的实例
     * <p>
     * 该方法主要用于生成一个带有操作信息的结果对象，用于记录某个用户在某个时间点进行了某个操作
     *
     * @param op     操作类型
     * @param userId 用户ID，执行操作的用户唯一标识
     * @param user   用户名，执行操作的用户的可读名称
     * @param msg    操作消息，对操作的简要描述
     * @return 返回一个填充了操作信息的ResultOperationsVo实例
     * @see com.tapdata.tm.taskinspect.vo.ResultOperationsVo#OP_CDC_CHECK
     */
    public static ResultOperationsVo create(String op, String userId, String user, String msg) {
        ResultOperationsVo ins = new ResultOperationsVo();
        ins.setTs(System.currentTimeMillis());
        ins.setOp(op);
        ins.setUserId(userId);
        ins.setUser(user);
        ins.setMsg(msg);
        return ins;
    }

}
