package io.tapdata.entity.aspect;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class Aspect {
    protected long time;

    public Aspect() {
        time = System.currentTimeMillis();
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
