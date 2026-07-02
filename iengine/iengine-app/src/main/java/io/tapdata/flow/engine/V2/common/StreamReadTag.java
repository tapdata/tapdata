package io.tapdata.flow.engine.V2.common;

import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.TapdataEvent;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 15:44 Create
 * @description
 */
public final class StreamReadTag {
    public Consumer<List<String>> startStreamRead;
    public Function<List<String>, List<String>> targetTableName;

    public StreamReadTag(Function<List<String>, List<String>> targetTableName, Consumer<List<String>> startStreamRead) {
        this.startStreamRead = startStreamRead;
        this.targetTableName = targetTableName;
    }

    public void accept(TapdataEvent e) {
        if (startStreamRead == null) {
            return;
        }
        if (!(e instanceof TapdataCompleteTableSnapshotEvent tableSnapshotEvent)) {
            return;
        }
        List<String> tableName = this.targetTableName.apply(List.of(tableSnapshotEvent.getSourceTableName()));
        this.startStreamRead.accept(tableName);
    }

    public void accept(List<String> tableNames) {
        if (startStreamRead == null) {
            return;
        }
        this.startStreamRead.accept(tableNames);
    }
}
