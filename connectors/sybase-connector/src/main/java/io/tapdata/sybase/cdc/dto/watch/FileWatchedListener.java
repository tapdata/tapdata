package io.tapdata.sybase.cdc.dto.watch;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

public interface FileWatchedListener {
    default void onCreated(WatchEvent<Path> watchEvent) {
    }

    default void onDeleted(WatchEvent<Path> watchEvent) {
    }

    void onModified(WatchEvent<Path> watchEvent);

    default void onOverflowed(WatchEvent<Path> watchEvent) {
    }
}

