package com.tapdata.tm.alarm.constant;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
public class AlarmContentTemplate {
    /**
     * - 告警格式：【级别】任务[${任务名}]运行出现错误，请及时处理！告警时间：${告警发生时间}
     * - 告警样例：【Emergency】任务[MySQL-2-Oracle]运行出现错误，请及时处理！告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_ERROR = "任务[$taskName]运行出现错误，请及时处理！告警时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]的数据检验任务出错，请关注！告警时间：${告警发生时间}
     * - 告警样例：【Warning】任务[MySQL-2-Oracle]的数据检验任务出错，请关注！告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_INSPECT_ERROR = "任务[$taskName]的数据检验任务出错，请关注！告警时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]全量同步已完成，耗时${全量耗时}。告警时间：${全量完成时间}
     * - 告警样例：【Normal】任务[MySQL-2-Oracle]全量同步已完成，耗时2h30min。告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_FULL_COMPLETE = "任务[$taskName]全量同步已完成，耗时{0}ms, 全量完成时间点：{1},告警时间：{2}";

    /**
     * - 告警格式：【级别】任务[${任务名}]增量同步已开始。告警时间：${增量开始时间}
     * - 告警样例：【Normal】任务[MySQL-2-Oracle]增量同步已开始。告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_INCREMENT_START = "任务[$taskName]增量同步已开始, 增量时间点{0}, 告警时间：{1}";

    /**
     * - 告警格式：【级别】任务[${任务名}]已出错停止，请尽快处理！告警时间：${出错停止时间}
     * - 告警样例：【Emergency】任务[MySQL-2-Oracle]已出错停止，请尽快处理！告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_STOP_ERROR = "任务[$taskName]已出错停止，请尽快处理！告警时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]已被用户[${停止任务的用户名}]停止，请关注！告警时间：${停止时间}
     * - 告警样例：【Warning】任务[MySQL-2-Oracle]已被用户[majp]停止，请关注！告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_STOP_MANUAL = "任务[$taskName]已被用户[{0}]停止，请关注！告警时间：{1}";

    /**
     * 【Warning】任务[MySQL-2-Oracle]增量延迟大于阈值500ms，当前值：628ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 增量延迟大于阈值500ms，已持续5分钟，当前值：928ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 增量延迟已恢复正常，当前值：28ms。告警时间：2022-08-08 12:23:12
     */
    public static final String TASK_INCREMENT_DELAY_START = "任务[$taskName]增量延迟{3}阈值{0}ms，当前值：{1}ms，请关注！告警时间：{2}";
    public static final String TASK_INCREMENT_DELAY_ALWAYS = "任务[$taskName]增量延迟{4}阈值{0}ms，已持续{1}分钟，当前值：{2}ms，请关注！告警时间：{3}";
    public static final String TASK_INCREMENT_DELAY_RECOVER = "任务[$taskName]增量延迟已恢复正常，当前值：{0}ms。告警时间：{1}";

    /**
     * 【Critical】任务[$taskName] 使用的源连接 [MySQL-Test]当前无法正常连接，请尽快处理！告警时间：2022-08-08 12:23:12
     * 【Critical】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]当前无法正常连接，已持续5分钟，请尽快处理！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]已恢复正常连接。告警时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_SOURCE_CANNOT_CONNECT = "任务[$taskName] 使用的源连接 [{0}]当前无法正常连接，请尽快处理！告警时间：{1}";
    public static final String DATANODE_SOURCE_CANNOT_CONNECT_ALWAYS = "任务[$taskName] 使用的源连接 [{0}]当前无法正常连接，已持续{1}分钟，请尽快处理！告警时间：{2}";
    public static final String DATANODE_SOURCE_CANNOT_CONNECT_RECOVER = "任务[$taskName] 使用的源连接 [{0}]已恢复正常连接。告警时间：{1}";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]网络连接耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的源连接 [MySQL-Test]网络连接耗时已恢复正常，当前值：99ms。告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接[ MySQL-Test]网络连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_HTTP_CONNECT_CONSUME_START = "";
    public static final String DATANODE_HTTP_CONNECT_CONSUME_ALWAYS = "";
    public static final String DATANODE_HTTP_CONNECT_CONSUME_RECOVER = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]协议连接耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]协议连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的源连接 [MySQL-Test]协议连接耗时已恢复正常，当前值：99ms。告警时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_TCP_CONNECT_CONSUME_START = "";
    public static final String DATANODE_TCP_CONNECT_CONSUME_ALWAYS = "";
    public static final String DATANODE_TCP_CONNECT_CONSUME_RECOVER = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 的源节点[CUSTOMER]平均处理耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 的源节点[CUSTOMER]平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的源节点[CUSTOMER] 平均处理耗时已恢复正常，当前值：9ms。告警时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_AVERAGE_HANDLE_CONSUME_START = "任务[$taskName]的源节点[{0}]平均处理耗时{4}阈值{1}ms，当前值：{2}ms，请关注！告警时间：{3}";
    public static final String DATANODE_AVERAGE_HANDLE_CONSUME_ALWAYS = "任务[$taskName]的源节点[{0}]平均处理耗时{5}阈值{1}ms，已持续{2}分钟，当前值：{3}ms，请关注！告警时间：{4}";
    public static final String DATANODE_AVERAGE_HANDLE_CONSUME_RECOVER = "任务[$taskName]的源节点[{0}]平均处理耗时已恢复正常，当前值：{1}ms。告警时间：{2}";

    /**
     * 【Warning】任务[MySQL-2-Oracle]的处理节点[JS节点] 平均处理耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle]的处理节点[JS节点] 平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的处理节点[JS节点] 平均处理耗时已恢复正常，当前值：9ms。告警时间：2022-08-08 12:23:12
     */
    public static final String PROCESSNODE_AVERAGE_HANDLE_CONSUME_START = "任务[$taskName]的处理节点[{0}]平均处理耗时{4}阈值{1}ms，当前值：{2}ms，请关注！告警时间：{3}";
    public static final String PROCESSNODE_AVERAGE_HANDLE_CONSUME_ALWAYS = "任务[$taskName]的处理节点[{0}]平均处理耗时{5}阈值{1}ms，已持续{2}分钟，当前值：{3}ms，请关注！告警时间：{4}";
    public static final String PROCESSNODE_AVERAGE_HANDLE_CONSUME_RECOVER = "任务[$taskName]的处理节点[{0}]平均处理耗时已恢复正常，当前值：{1}ms。告警时间：{2}";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]网络连接耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接[ Oracle-Test]网络连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的目标连接 [Oracle-Test]网络连接耗时已恢复正常，当前值：99ms。告警时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]协议连接耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]协议连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的目标连接 [Oracle-Test]协议连接耗时已恢复正常，当前值：99ms。告警时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER]平均处理耗时超过阈值500ms，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER]平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！告警时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER] 平均处理耗时已恢复正常，当前值：9ms。告警时间：2022-08-08 12:23:12
     */
    public static final String TARGET_AVERAGE_HANDLE_CONSUME_START = "任务[$taskName]的目标节点[{0}]平均处理耗时{4}阈值{1}ms，当前值：{2}ms，请关注！告警时间：{3}";
    public static final String TARGET_AVERAGE_HANDLE_CONSUME_ALWAYS = "任务[$taskName]的目标节点[{0}]平均处理耗时{5}阈值{1}ms，已持续{2}分钟，当前值：{3}ms，请关注！告警时间：{4}";
    public static final String TARGET_AVERAGE_HANDLE_CONSUME_RECOVER = "任务[$taskName]的目标节点[{0}]平均处理耗时已恢复正常，当前值：{1}ms。告警时间：{2}";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 所在Agent[Agent01]已停止运行，当前还有2个可用Agent，任务将重新调度到Agent[Agent02]上运行，请关注！告警时间：2022-08-08 12:23:12
     */
    public static final String SYSTEM_FLOW_EGINGE_DOWN_CHANGE_AGENT = "任务[$taskName]所在Agent[{0}]已停止运行，当前还有{1}个可用Agent，任务将重新调度到Agent[{2}]上运行，请关注！告警时间：{3}";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 所在Agent[Agent01]已停止运行，当前已无可用Agent，任务已停止运行，请尽快处理！告警时间：2022-08-08 12:23:12
     */
    public static final String SYSTEM_FLOW_EGINGE_DOWN_NO_AGENT = "任务[$taskName]所在Agent[{0}]已停止运行，当前已无可用Agent，将会影响任务正常运行，请尽快处理！告警时间：{1}";

}
