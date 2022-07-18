package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author samuel
 * @Description
 * @create 2022-03-02 01:31
 **/
public abstract class TaskMonitor<T> implements Monitor<T> {

  private static final Long INTERVAL_MS = 2000L;

  protected Long intervalMs;
  protected SubTaskDto subTaskDto;
  protected Logger logger = LogManager.getLogger(TaskMonitor.class);

  public TaskMonitor(SubTaskDto subTaskDto) {
    assert null != subTaskDto;
    this.subTaskDto = subTaskDto;
    Log4jUtil.setThreadContext(subTaskDto);
    this.intervalMs = INTERVAL_MS;
  }

  @Override
  public T get() {
    return null;
  }
}
