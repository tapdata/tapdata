package io.tapdata.task;

public class TaskResult {

	private int taskResultCode;

	private Object taskResult;

	public void setFailedResult(Object taskResult) {
		this.setTaskResultCode(201);
		this.setTaskResult(taskResult);
	}

	public void setPassResult() {
		this.setTaskResultCode(200);
	}

	public int getTaskResultCode() {
		return taskResultCode;
	}

	public void setTaskResultCode(int taskResultCode) {
		this.taskResultCode = taskResultCode;
	}

	public Object getTaskResult() {
		return taskResult;
	}

	public void setTaskResult(Object taskResult) {
		this.taskResult = taskResult;
	}

	@Override
	public String toString() {
		return "TaskResult{" +
				"taskResultCode=" + taskResultCode +
				", taskResult=" + taskResult +
				'}';
	}
}
