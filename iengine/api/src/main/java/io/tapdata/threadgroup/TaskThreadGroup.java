package io.tapdata.threadgroup;

import com.tapdata.tm.commons.task.dto.TaskDto;

public class TaskThreadGroup extends ThreadGroup {
    protected TaskDto taskDto;
    public TaskThreadGroup(TaskDto taskDto) {
        super("TaskThreadGroup-" + taskDto.getName());
        this.taskDto = taskDto;
    }

    public TaskDto getTaskDto() {
        return taskDto;
    }

    public void setTaskDto(TaskDto taskDto) {
        this.taskDto = taskDto;
    }
}
