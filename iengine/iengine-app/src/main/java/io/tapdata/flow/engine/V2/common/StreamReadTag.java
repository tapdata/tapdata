package io.tapdata.flow.engine.V2.common;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataStartedCdcEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 15:44 Create
 * @description
 */
public final class StreamReadTag {
    final int sourceNodeCount;
    public List<Runnable> startStreamRead;
    int acceptedCount;

    public StreamReadTag(int sourceNodeCount, Runnable ... startStreamRead) {
        this.sourceNodeCount = sourceNodeCount;
        if (startStreamRead != null) {
            this.startStreamRead = Arrays.asList(startStreamRead);
        } else {
            this.startStreamRead = new ArrayList<>();
        }
        this.acceptedCount = 0;
    }

    public void accept(TapdataEvent e) {
        if (this.acceptedCount >= this.sourceNodeCount) {
            return;
        }
        if (!(e instanceof TapdataStartedCdcEvent)) {
            return;
        }
        this.acceptedCount++;
        if (this.acceptedCount >= this.sourceNodeCount) {
            this.startStreamRead.forEach(Runnable::run);
        }
    }
}
