package com.tapdata.tm.sdk.available;


import com.tapdata.tm.sdk.util.AppType;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class TmStatusService {

  private final static AppType appType = AppType.init();
  private final static AtomicBoolean available = new AtomicBoolean(true);

  private final static Map<String, AtomicBoolean> taskReportStatusMap = new ConcurrentHashMap<>();

  private final static List<Runnable> toAvailableHandler = new ArrayList<>();

  public static boolean isEnable() {
    return appType.isCloud();
  }

  public static boolean isNotEnable() {
    return !isEnable();
  }
  public static boolean isNotAvailable() {
    return !isAvailable();
  }
  public static boolean isAvailable() {
    if (isNotEnable()) {
      return true;
    }
    return available.get();
  }

  public static void setNotAvailable() {
    if (isNotEnable()) {
      return;
    }
    available.set(false);
    for (AtomicBoolean taskReportStatus : taskReportStatusMap.values()) {
      taskReportStatus.set(false);
    }
  }

  /**
   * to available, do something...
   */
  public static void setAvailable() {
    if (isNotEnable()) {
      return;
    }
    available.set(true);
    if (!toAvailableHandler.isEmpty()) {
      toAvailableHandler.forEach(Runnable::run);
    }
  }

  public static void addNewTask(String taskId) {
    if (isNotEnable()) {
      return;
    }
    if (isAvailable()) {
      taskReportStatusMap.put(taskId, new AtomicBoolean(true));
    } else {
      taskReportStatusMap.put(taskId, new AtomicBoolean(false));
    }
  }

  public static void setAllowReport(String taskId) {
    if (isNotEnable()) {
      return;
    }
    taskReportStatusMap.computeIfAbsent(taskId, k -> new AtomicBoolean()).set(true);
  }

  public static boolean isNotAllowReport() {
    return !isAllowReport();
  }

  public static boolean isAllowReport() {
    String taskId = ThreadContext.get("taskId");
    return isAllowReport(taskId);
  }

  public static boolean isNotAllowReport(String taskId) {
    return !isAllowReport(taskId);
  }

  public static boolean isAllowReport(String taskId) {
    if (isNotEnable()) {
      return true;
    }
    if (StringUtils.isEmpty(taskId)) {
      return true;
    }
    AtomicBoolean status = taskReportStatusMap.get(taskId);
    return status == null || status.get();
  }

  public static void registeredTmAvailableHandler(Runnable runnable) {
    if (isNotEnable()) {
      return;
    }
    toAvailableHandler.add(runnable);
  }

  public static void removeTask(String taskId) {
    if (isNotEnable()) {
      return;
    }
    taskReportStatusMap.remove(taskId);
  }
}
