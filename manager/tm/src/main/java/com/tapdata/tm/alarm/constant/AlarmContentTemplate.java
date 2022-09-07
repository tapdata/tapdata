package com.tapdata.tm.alarm.constant;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
public class AlarmContentTemplate {
    /**
     * - 告警格式：【级别】任务[${任务名}]运行出现错误，请及时处理！时间：${告警发生时间}
     * - 告警样例：【Emergency】任务[MySQL-2-Oracle]运行出现错误，请及时处理！时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_ERROR = "任务[${taskName}]运行出现错误，请及时处理！时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]的数据检验任务出错，请关注！时间：${告警发生时间}
     * - 告警样例：【Warning】任务[MySQL-2-Oracle]的数据检验任务出错，请关注！时间：2022-08-08 12:23:12
     */
    public static final String TASK_INSPECT_ERROR = "任务[${taskName}]的数据检验任务出错，请关注！时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]全量同步已完成，耗时${全量耗时}。时间：${全量完成时间}
     * - 告警样例：【Normal】任务[MySQL-2-Oracle]全量同步已完成，耗时2h30min。时间：2022-08-08 12:23:12
     */
    public static final String TASK_FULL_COMPLETE = "任务[${taskName}]全量同步已完成，耗时{0}。时间：{1}";

    /**
     * - 告警格式：【级别】任务[${任务名}]增量同步已开始。时间：${增量开始时间}
     * - 告警样例：【Normal】任务[MySQL-2-Oracle]增量同步已开始。时间：2022-08-08 12:23:12
     */
    public static final String TASK_INCREMENT_COMPLETE = "任务[${taskName}]增量同步已开始。时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]已出错停止，请尽快处理！时间：${出错停止时间}
     * - 告警样例：【Emergency】任务[MySQL-2-Oracle]已出错停止，请尽快处理！时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_STOP_ERROR = "任务[${taskName}]已出错停止，请尽快处理！时间：{0}";

    /**
     * - 告警格式：【级别】任务[${任务名}]已被用户[${停止任务的用户名}]停止，请关注！时间：${停止时间}
     * - 告警样例：【Warning】任务[MySQL-2-Oracle]已被用户[majp]停止，请关注！时间：2022-08-08 12:23:12
     */
    public static final String TASK_STATUS_STOP_MANUAL = "任务[${taskName}]已被用户[{0}]停止，请关注！时间：{1}";

    /**
     * 【Warning】任务[MySQL-2-Oracle]增量延迟大于阈值500ms，当前值：628ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 增量延迟已恢复正常，当前值：28ms。时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 增量延迟大于阈值500ms，已持续5分钟，当前值：928ms，请关注！时间：2022-08-08 12:23:12
     */
    public static final String TASK_INCREMENT_DELAY_START = "";
    public static final String TASK_INCREMENT_DELAY_ALWAYS = "";
    public static final String TASK_INCREMENT_DELAY_RECOVER = "";

    /**
     * 【Critical】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]当前无法正常连接，请尽快处理！时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_CANNOT_CONNECT = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]网络连接耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的源连接 [MySQL-Test]网络连接耗时已恢复正常，当前值：99ms。时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接[ MySQL-Test]网络连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_HTTP_CONNECT_CONSUME_START = "";
    public static final String DATANODE_HTTP_CONNECT_CONSUME_ALWAYS = "";
    public static final String DATANODE_HTTP_CONNECT_CONSUME_RECOVER = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]协议连接耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的源连接 [MySQL-Test]协议连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的源连接 [MySQL-Test]协议连接耗时已恢复正常，当前值：99ms。时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_TCP_CONNECT_CONSUME_START = "";
    public static final String DATANODE_TCP_CONNECT_CONSUME_ALWAYS = "";
    public static final String DATANODE_TCP_CONNECT_CONSUME_RECOVER = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle] 的源节点[CUSTOMER]平均处理耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 的源节点[CUSTOMER]平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的源节点[CUSTOMER] 平均处理耗时已恢复正常，当前值：9ms。时间：2022-08-08 12:23:12
     */
    public static final String DATANODE_AVERAGE_HANDLE_CONSUME_START = "";

    /**
     * 【Warning】任务[MySQL-2-Oracle]的处理节点[JS节点] 平均处理耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle]的处理节点[JS节点] 平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的处理节点[JS节点] 平均处理耗时已恢复正常，当前值：9ms。时间：2022-08-08 12:23:12
     */

    /**
     * 【Critical】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]当前无法正常连接，请尽快处理！时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]网络连接耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接[ Oracle-Test]网络连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的目标连接 [Oracle-Test]网络连接耗时已恢复正常，当前值：99ms。时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]协议连接耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 使用的目标连接 [Oracle-Test]协议连接耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle]使用的目标连接 [Oracle-Test]协议连接耗时已恢复正常，当前值：99ms。时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER]平均处理耗时超过阈值500ms，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Warning】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER]平均处理耗时超过阈值500ms，已持续5分钟，当前值：899ms，请关注！时间：2022-08-08 12:23:12
     * 【Recovery】任务[MySQL-2-Oracle] 的目标节点[CUSTOMER] 平均处理耗时已恢复正常，当前值：9ms。时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 所在Agent[Agent01]已停止运行，当前还有2个可用Agent，任务将重新调度到Agent[Agent02]上运行，请关注！时间：2022-08-08 12:23:12
     */

    /**
     * 【Warning】任务[MySQL-2-Oracle] 所在Agent[Agent01]已停止运行，当前已无可用Agent，任务已停止运行，请尽快处理！时间：2022-08-08 12:23:12
     */

}
