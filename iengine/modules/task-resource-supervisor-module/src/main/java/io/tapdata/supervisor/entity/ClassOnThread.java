package io.tapdata.supervisor.entity;

import io.tapdata.supervisor.util.TapTaskThreadGroupUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClassOnThread {
    private Long time;
    private String threadName;
    private Long threadId;
    private Object thisObj;
    private List<String> stackTrace;
    private ThreadGroup threadGroup;

    public static ClassOnThread create() {
        return new ClassOnThread();
    }

    public ClassOnThread time(long time) {
        this.time = time;
        return this;
    }

    public ClassOnThread thread(Thread thread) {
        this.threadId = thread.getId();
        this.threadName = thread.getName();
        this.threadGroup = TapTaskThreadGroupUtil.getDefaultThreadUtil().currentThreadGroup(thread, TapTaskThreadGroupUtil.getDefaultNodeThreadGroupClass());
        return this;
    }

    public ClassOnThread stackTrace(List<StackTraceElement> stackTrace) {
        this.stackTrace = stackTrace.stream()
                .filter(Objects::nonNull)
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
        return this;
    }

    public ClassOnThread addStackTrace(StackTraceElement stackTrace) {
        if (Objects.isNull(this.stackTrace)) {
            this.stackTrace = new ArrayList<>();
        }
        this.stackTrace.add(stackTrace.toString());
        return this;
    }

    public ClassOnThread thisObj(Object thisObj) {
        this.thisObj = thisObj;
        return this;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getThreadName() {
        return threadName;
    }

    public Long getThreadId() {
        return threadId;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public void setThreadId(Long threadId) {
        this.threadId = threadId;
    }

    public Object getThisObj() {
        return thisObj;
    }

    public void setThisObj(Object thisObj) {
        this.thisObj = thisObj;
    }

    public List<String> getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(List<StackTraceElement> stackTrace) {
        this.stackTrace(stackTrace);
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }
}