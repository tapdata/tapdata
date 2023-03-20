package io.tapdata;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 12:08
 **/
@TapExClass(code = 10, module = "PDK", prefix = "PDK", describe = "Error code for PDK")
public interface PDKExCode_10 {

	@TapExCode(
			describe = "",
			describeCN = "客户端进程被服务端关闭\n原因：\n1. ",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String TERMINATE_BY_SERVER = "10001";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String USERNAME_PASSWORD_INVALID = "10002";

	@TapExCode(
			describe = "This error occurs during the startup increment phase. The start point no longer exists in the source database log. In most cases, the start point is the time or the log ID of the source database. \n\n" +
					"For most databases, the incremental function is implemented based on the database log file. The engine completes the incremental reading phase by listening to or actively reading the database log file. \n" +
					"So before reading, the engine needs to find the specific location in the log to listen to or read the log file based on the starting point. If it cannot find the location in the log file, it will cause an error. \n\n" +
					"Cause \n" +
					"1. The source database manually or periodically clears log files;\n" +
					"2. If the task is an incremental task, the value of \"Incremental collection start time\" in the task setting is incorrect, or the time zone does not match the database time zone;\n" +
					"3. If the increment speed is too slow, the delay is too large. In this case, the source database writes the oldest logs, for example, the MongoDB Oplog.",
			solution = "1. Reset the task and start it again. In this case, the task will be reinitialized and enter the increment phase smoothly\n" +
					"2. After the task is reset, change the task to the incremental mode, set the incremental start time, and start the task again. \n" +
					"In this solution, you need to manually query the earliest log time in the source database and ensure that the set delta start point is included in the log file. Otherwise, some delta data will be lost.",
			describeCN = "该错误发生在启动增量阶段，启动起始点在源库日志中已经不存在，大部分情况起始点为时间或者源库的日志ID。\n\n" +
					"对于大部分数据库，增量功能是基于数据库的日志文件实现的，引擎通过监听或者主动读取数据库日志文件，完成增量的读取阶段。\n" +
					"所以在读取前，引擎需要在日志中，依据起始点找到监听或者读取日志文件的具体位置，无法在日志文件中找到位置时，则会导致错误。\n\n" +
					"原因\n" +
					"1. 源库执行了日志文件的人工或定时清理；\n" +
					"2. 如果是增量任务，任务设置中\"增量采集开始时刻\"，设置错误，或者时区与数据库时区不匹配导致的错误；\n" +
					"3. 增量速度过慢，导致延迟过大，此时源库的写入覆盖了最旧的日志，比如MongoDB的Oplog，可以尝试调大日志空间，或者排查增量速度过慢的原因。",
			solutionCN = "1. 重置任务后再次启动，此时任务会重新初始化并顺利进入增量阶段\n" +
					"2. 重置任务后，将任务改为增量模式，设置增量起始时间后，再次启动任务。\n" +
					"该方案需要人工的在源库中查询出最早的日志时间，确保设置的增量起始点包含在日志文件中，否则会丢失一部分增量数据。",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String OFFSET_OUT_OF_LOG = "10003";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String READ_MISSING_PRIVILEGES = "10004";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_MISSING_PRIVILEGES = "10005";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_TYPE = "10006";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_LENGTH_INVALID = "10007";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_VIOLATE_UNIQUE_CONSTRAINT = "10008";

	@TapExCode(
			describe = "",
			describeCN = "",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_VIOLATE_NULLABLE_CONSTRAINT = "10009";
}
