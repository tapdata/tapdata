package io.tapdata.task;

import com.tapdata.entity.ScheduleTask;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "JS_FUNC")
public class ScriptTask implements Task {

	private Logger logger = LogManager.getLogger(getClass());

	private static final String TASK_SCRIPT_DIRECTORY = "task-scripts";

	private static final String BUILT_IN_SCRIPT = "" +
			"logger = {};\n" +
			"logger.logMsg = \"\";\n" +
			"logger.log = function(msg){var timeString = new ISODate().toLocaleString(); print(timeString + \" \" + msg);};\n" +
			"function eval_helper(){" +
			"   load(\"${SCRIPT_WORK_DIR}/${FUNCTION_NAME}.js\");" +
			"   ${FUNCTION_NAME}(${PARAMENTS});" +
//            "   var resultCode = 200;" +
//            "   return {\"resultCode\": resultCode, \"logs\": logger.logMsg};" +
			"}\n" +
			"eval_helper();";

	private Map<String, Object> tapdataBulitInData;

	private ScheduleTask scheduleTask;

	private String scheduleScript;

	private String executeMongoShellCommand;

	private ProcessBuilder processBuilder;

	@Override
	public void initialize(TaskContext taskContext) {
		try {
			tapdataBulitInData = taskContext.getTapdataBulitInData();
			scheduleTask = taskContext.getScheduleTask();

			String metaDBParams = (String) tapdataBulitInData.get(TaskContext.META_DB_PARAMS);

			String scriptWorkDIR = null;
			String userDIR = System.getProperty("user.dir");
			File file = new File(userDIR + "/" + TASK_SCRIPT_DIRECTORY);
			if (file.exists() && file.isDirectory()) {
				scriptWorkDIR = userDIR + "/" + TASK_SCRIPT_DIRECTORY;
			} else {
				scriptWorkDIR = userDIR + "/etc/" + TASK_SCRIPT_DIRECTORY;
			}

			scheduleScript = scheduleScript(scheduleTask, scriptWorkDIR);

			String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
			if (StringUtils.isBlank(tapdataWorkDir)) {
				tapdataWorkDir = userDIR;
			}

			logger.info("Script task {} work directory is {}.", scheduleTask.getTask_name(), scriptWorkDIR);

			executeMongoShellCommand = "cd " + scriptWorkDIR + " && mongo '" + metaDBParams + "' --eval '" + scheduleScript + "'";
			processBuilder = new ProcessBuilder();
			processBuilder.command("bash", "-c", executeMongoShellCommand);

			String logDIRPath = tapdataWorkDir + "/logs";
			File logDIR = new File(logDIRPath);
			if (!logDIR.exists() || !logDIR.isDirectory()) {
				logDIR.mkdir();
			}
			File logFile = new File(logDIRPath + "/" + scheduleTask.getTask_name() + ".log");
			if (logFile.exists()) {
				logFile.delete();
			}
			logFile.createNewFile();
			processBuilder.redirectOutput(logFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		Process p = null;
//    try {
////            Document result = metaDatabase.runCommand(new Document("$eval", scheduleScript).append("nolock", true), Document.class);
////            Document retVal = result.get("retval", Document.class);
//
////            Process p = Runtime.getRuntime().exec(executeMongoShellCommand);
//      p = processBuilder.start();
//      p.waitFor();
//      taskResult.setTaskResultCode(p.exitValue());
//      taskResult.setTaskResult(IoUtil.read(p.getInputStream()));
//    } catch (Exception e) {
//      String message = e.getMessage();
//      taskResult.setTaskResult(message);
//      taskResult.setTaskResultCode(201);
//    } finally {
//      if (p != null) {
//        if (p.isAlive()) {
//          p.destroyForcibly();
//        }
//      }
//    }

		callback.accept(taskResult);
	}

	private String scheduleScript(ScheduleTask scheduleTask, String scriptWorkDIR) {
		Map<String, Object> taskData = scheduleTask.getTask_data();
		String scheduledScript = null;
		if (taskData.containsKey("task_handle")) {
			String taskHandle = (String) taskData.get("task_handle");

			if (StringUtils.isNotBlank(taskHandle)) {
				scheduledScript = StringUtils.replace(BUILT_IN_SCRIPT, "${FUNCTION_NAME}", taskHandle);
				scheduledScript = StringUtils.replace(scheduledScript, "${SCRIPT_WORK_DIR}", scriptWorkDIR);
			}
		}

		if (taskData.containsKey("task_arguments")) {
			List<String> taskArguments = (List<String>) taskData.get("task_arguments");

			if (CollectionUtils.isNotEmpty(taskArguments)) {
				StringBuilder parameters = new StringBuilder();
				for (String taskArgument : taskArguments) {
					if (tapdataBulitInData.containsKey(taskArgument)) {
						taskArgument = String.valueOf(tapdataBulitInData.get(taskArgument));
					}
					parameters.append("\"").append(taskArgument).append("\"").append(",");
				}
				parameters.replace(parameters.length() - 1, parameters.length(), "");
				scheduledScript = StringUtils.replace(
						scheduledScript, "${PARAMENTS}", parameters.toString()
				);
			} else {
				scheduledScript = StringUtils.replace(
						scheduledScript, "${PARAMENTS}", ""
				);
			}
		}

		return scheduledScript;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
//        ProcessBuilder processBuilder = new ProcessBuilder();
//        processBuilder.command("bash", "-c", "mongo mongodb://127.0.0.1:27017/tapdata --eval 'logger = {};logger.logMsg = \"\";logger.log = function(msg){print(msg);};function eval_helper(){ load(\"task-scripts/meta_data_analyzer.js\");   meta_data_analyzer();}eval_helper();'\n");
//
//        processBuilder.inheritIO();
//        Process start = processBuilder.start();
//
//
//        start.waitFor();
//
//        while (true) {
//            Thread.sleep(1000);
//        }
		System.out.println(new File(".").getCanonicalPath());
		System.out.println(System.getProperty("user.dir"));
	}
}
