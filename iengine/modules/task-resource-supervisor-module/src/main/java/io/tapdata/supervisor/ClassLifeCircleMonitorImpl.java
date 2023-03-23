package io.tapdata.supervisor;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.supervisor.entity.ClassOnThread;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Implementation(ClassLifeCircleMonitor.class)
public class ClassLifeCircleMonitorImpl implements ClassLifeCircleMonitor<ClassOnThread> {
    private final Map<Object, ClassOnThread> classOnThreadMap = new ConcurrentHashMap<>();

    @Override
    public void instanceStarted(Object thisObj) {
        if (Objects.nonNull(thisObj) && !this.classOnThreadMap.containsKey(thisObj)) {
            Thread thread = Thread.currentThread();
            this.classOnThreadMap.put(
                    thisObj,
                    ClassOnThread.create()
                            .thisObj(thisObj)
                            .thread(thread)
                            .time(System.nanoTime())
                            .stackTrace(new ArrayList<>(Arrays.asList(thread.getStackTrace())))
            );
        }
    }

    @Override
    public void instanceEnded(Object thisObj) {
        if (Objects.nonNull(thisObj)) {
            this.classOnThreadMap.remove(thisObj);
        }
    }

    @Override
    public Map<Object, ClassOnThread> summary() {
        return this.classOnThreadMap;
    }
}