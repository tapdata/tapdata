package io.tapdata;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 12:08
 **/
@TapExClass(code = 10, module = "PDK", prefix = "PDK", describe = "PDK connectors")
public interface PDKExCode_10 {

	@TapExCode(
			describe = "The client connection was closed by the server.\n\n" +
					"Reason\n1. The server manually closed the connection;\n" +
					"2. The server has too many connections, automatically closed or rejected subsequent connections.",
			describeCN = "客户端连接被服务端关闭。\n\n" +
					"原因\n1. 服务端手动关闭了连接；\n" +
					"2. 服务端连接数过多，自动关闭或者拒绝了后续连接。",
			solution = "",
			solutionCN = "",
			recoverable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String TERMINATE_BY_SERVER = "10001";

	@TapExCode(
			describe = "Username or password error.\n\nReason\n1. Wrong filling;\n2. The password contains special characters.",
			describeCN = "用户名或者密码错误。\n\n原因\n1. 填写错误；\n2. 密码中含有特殊字符。",
			solution = "1. Try to fill in the password again, and conduct a connection test;\n2. Try to change the password without special characters, and report the issue to Tapdata",
			solutionCN = "1. 尝试重新填写密码，并进行连接测试；\n2. 尝试换一个没有特殊字符的密码，并将该情况汇报给Tapdata",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String USERNAME_PASSWORD_INVALID = "10002";

	@TapExCode(
			describe = "This error occurs during the startup increment phase. The start point no longer exists in the source database log. In most cases, the start point is the time or the log ID of the source database. \n\n" +
					"For most databases, the incremental function is implemented based on the database log file. The engine completes the incremental reading phase by listening to or actively reading the database log file. \n" +
					"So before reading, the engine needs to find the specific location in the log to listen to or read the log file based on the starting point. If it cannot find the location in the log file, it will cause an error. \n\n" +
					"Reason\n" +
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
			describe = "When reading data, the corresponding permission is missing. \n\n" +
					"Reason\n1. The user in the data connection used does not have the corresponding read permission;\n" +
					"2. For some databases, more permissions are required for incremental reading. Please refer to the instructions on the right side of creating a data source. Confirm that the permissions are set correctly.",
			describeCN = "读取数据时，缺失了相应权限。\n\n原因\n1. 使用的数据连接中的用户，没有相应的读取权限；\n2. 部分数据库，增量读取所需的权限较多，请查看创建数据源右侧的说明，确认权限是否设置正确。",
			solution = "",
			solutionCN = "",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String READ_MISSING_PRIVILEGES = "10004";

	@TapExCode(
			describe = "When writing data, the corresponding permission is missing.",
			describeCN = "写入数据时，缺失了相应权限。",
			solution = "Check whether the user name in the data connection used by the target node lacks write permission.",
			solutionCN = "检查目标节点所使用的数据连接中用户名是否缺失了写入权限。",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_MISSING_PRIVILEGES = "10005";

	@TapExCode(
			describe = "The written data type is inconsistent with the actual field type of the database.\n\nReason\n" +
					"1. Before running the task, the target table name already exists in the database, and Tapdata will not automatically create the table, which may be the reason The fields in some tables are inconsistent with the source table;\n" +
					"2. The source database is an unstructured database, and the target is a structured database. For example, when MongoDB synchronized to Oracle, a field at the source may have multiple types, but a field of the target structured database does not allow multiple types to be written, which will cause the error;\n" +
					"3. During the synchronization process, a computing node, such as a js processor, was added, which caused the data to change in the processing node type, triggering this error.",
			describeCN = "写入的数据类型与数据库实际的字段类型不一致。\n\n原因\n" +
					"1. 运行任务前，目标表名在数据库中已存在，Tapdata则不会自动创建该表，可能原有的表中字段存在与源表不一致的情况；\n" +
					"2. 源库是非结构化数据库，目标为结构化数据库，比如MongoDB同步到Oracle，源端的某一个字段可能有多个类型，而目标结构化数据库一个字段不允许多个类型写入，会导致该错误；\n" +
					"3. 同步过程中，加入了计算节点，如js处理器，导致数据在处理节点类型变化，触发了该错误。",
			solution = "1. Refer to the error message below, and compare the type of the wrong field in the source library and the target library. If not, use the DDL or similar commands to correct it, and then run the task again;\n" +
					"2. Use js processor, filter the error field, if the error field is field1, the corresponding js is \"record.remove('field1')\";\n" +
					"3. If the data type is modified by the js processor If you want to modify it, you should pass the new type to Tapdata at the bottom of the js edit box according to the model syntax, drop the target table, and run the task again.",
			solutionCN = "1. 参考下方报错信息，比对出错的字段在源库和目标库的类型是否一致，如果不一致，使用数据库的DDL或类似命令进行修正后，再次运行任务；\n" +
					"2. 使用js处理器，将出错字段过滤，如出错字段为field1，对应的js为\"record.remove('field1')\"；\n" +
					"3. 如果用js处理器对数据类型进行了改动，则应在js编辑框的下方，按照模型语法，将新的类型传递给Tapdata，删除目标表后，再次运行任务。",
			skippable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_TYPE = "10006";

	@TapExCode(
			describe = "The length of the written data is inconsistent with the actual field length of the database.\n\nReason\n" +
					"1. Before running the task, the target table name already exists in the database, and Tapdata will not automatically create the table, which may be the reason The fields in some tables are inconsistent with the source table;\n" +
					"2. The source database is an unstructured database, and the target is a structured database. For example, MongoDB synchronized to Oracle. The string type does not have a fixed length limit in MongoDB , and Oracle needs to specify the length when creating the table. At this time, it may cause an error of exceeding the length;\n" +
					"3. During the synchronization process, a computing node, such as a js processor, is added, resulting in a change in the data processing node type. triggered the error.",
			describeCN = "写入的数据长度与数据库实际的字段长度不一致。\n\n原因\n" +
					"1. 运行任务前，目标表名在数据库中已存在，Tapdata则不会自动创建该表，可能原有的表中字段存在与源表不一致的情况；\n" +
					"2. 源库是非结构化数据库，目标为结构化数据库，比如MongoDB同步到Oracle，字符串类型在MongoDB中并没有固定长度限制，而Oracle在创建表的时候需要指定长度。此时可能导致超出长度的报错；\n" +
					"3. 同步过程中，加入了计算节点，如js处理器，导致数据在处理节点变化，触发了该错误。",
			solution = "1. Refer to the error message below to find the wrong field, use the DDL or similar commands to correct it, and then run the task again;\n" +
					"2. Use the js processor to filter the wrong field, such as the wrong field is field1 , the corresponding js is \"record.remove('field1')\";\n" +
					"3. If the data type is changed with the js processor, it should be below the js edit box, according to the model syntax , pass the new length to Tapdata, delete the target table, and run the task again.",
			solutionCN = "1. 参考下方报错信息，找到出错的字段，使用数据库的DDL或类似命令进行修正后，再次运行任务；\n" +
					"2. 使用js处理器，将出错字段过滤，如出错字段为field1，对应的js为\"record.remove('field1')\"；\n" +
					"3. 如果用js处理器对字段值进行了改动，则应在js编辑框的下方，按照模型语法，将新的长度传递给Tapdata，删除目标表后，再次运行任务。",
			skippable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_LENGTH_INVALID = "10007";

	@TapExCode(
			describe = "Unique constraint was violated while writing data.\n\nReason\n" +
					"1. The unique index or primary key of the target table is inconsistent with the source table.",
			describeCN = "写入数据时，违反了唯一约束。\n\n原因\n" +
					"1. 目标表的唯一索引或主键与源表不一致。",
			solution = "1. Use database DDL or similar commands to modify the primary key or unique index of the target table, and then try to start the task again;\n" +
					"2. Delete the target table, let Tapdata's automatic table creation function recreate the table, and try to start again Task;\n" +
					"3. On the task editing interface of Tapdata, disable the concurrent writing of the target table, and try to start the task again.",
			solutionCN = "1. 使用数据库的DDL或类似命令，修改目标表的主键或唯一索引后，再次尝试启动任务；\n" +
					"2. 删除目标表，让Tapdata的自动建表功能重新创建表，再次尝试启动任务；\n" +
					"3. 在Tapdata的任务编辑界面，将目标表的并发写入关闭，再次尝试启动任务。",
			skippable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_VIOLATE_UNIQUE_CONSTRAINT = "10008";

	@TapExCode(
			describe = "When writing data, the non-null constraint was violated.\n\nReason\n" +
					"1. The field non-null constraint of the target table is inconsistent with the source table;\n" +
					"2. The js node is used, and the synchronization Some field values are set to null during the process, and at the same time, the field is a not-null constraint in the target table.",
			describeCN = "写入数据时，违反了非空约束。\n\n原因\n" +
					"1. 目标表的字段非空约束与源表不一致；\n" +
					"2. 使用了js节点，在同步过程中将某些字段值设置为空，同时目标表中，该字段又是非空约束。",
			solution = "1. Use the DLL of the database or similar commands to remove the non-null constraint of the target table, and then try to start the task again;\n" +
					"2. Check the js logic, whether the data value is changed to empty or the error field is deleted by mistake.",
			solutionCN = "1. 使用数据库的DLL或类似命令，去除目标表的非空约束后，再次尝试启动任务；\n" +
					"2. 检查js逻辑，是否错误的将数据值改为了空或删除了报错字段。",
			skippable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String WRITE_VIOLATE_NULLABLE_CONSTRAINT = "10009";

	@TapExCode(
			describe = "Due to the data itself, an error occurred: \n" +
					"1. When the error data tolerance function is enabled, error data can be skipped for processing.",
			describeCN = "由数据本身原因，导致错误：\n" +
					"1. 当开启错误数据容忍功能时，错误数据可被跳过处理；",
			solution = "1. Check the correctness of the data.",
			solutionCN = "1. 检查数据的正确性；",
			skippable = true,
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String SKIPPABLE_DATA = "10010";

	@TapExCode(
			describe = "Some configurations of the data source do not effectively support CDC functionality. Please refer to the exception details and suggestions for details;",
			describeCN = "数据源的某些配置并不能有效地支持CDC功能，具体可以查看异常详情与建议；",
			seeAlso = {"https://docs.tapdata.io/enterprise/user-guide/connect-database/"}
	)
	String DB_CDC_CONFIG_INVALID = "10011";


	@TapExCode(recoverable = true)
	String RETRYABLE_ERROR = "10012";
}
