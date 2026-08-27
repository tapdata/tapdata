package io.tapdata.task.skiperrortable;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;

class F07SkipErrorTableCompatibilityTest {

    @AfterEach
    void clearFactoryInstances() {
        synchronized (ISkipErrorTable.INSTANCES) {
            ISkipErrorTable.INSTANCES.clear();
        }
    }

    @Test
    void dqlAndLegacyTableModesDoNotCreateTaskSkipErrorTable() {
        HttpClientMongoOperator mongoOperator = new HttpClientMongoOperator(null, null, null, null);
        for (TaskDto.SkipErrorEvent.ErrorMode mode : List.of(
                TaskDto.SkipErrorEvent.ErrorMode.Disable,
                TaskDto.SkipErrorEvent.ErrorMode.SkipTable,
                TaskDto.SkipErrorEvent.ErrorMode.SkipData)) {
            TaskDto task = task(mode, TaskDto.SYNC_TYPE_MIGRATE);
            assertSame(ISkipErrorTable.EMPTY, ISkipErrorTable.create(task, mongoOperator), mode.name());
        }
    }

    @Test
    void migrateSnapshotModeIsNotEnabledForNonMigrateTasks() {
        TaskDto task = task(TaskDto.SkipErrorEvent.ErrorMode.SkipTableForMigrateSnapshot,
                TaskDto.SYNC_TYPE_SYNC);
        HttpClientMongoOperator mongoOperator = new HttpClientMongoOperator(null, null, null, null);

        assertSame(ISkipErrorTable.EMPTY, ISkipErrorTable.create(task, mongoOperator));
    }

    private TaskDto task(TaskDto.SkipErrorEvent.ErrorMode mode, String syncType) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setSyncType(syncType);
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode(mode);
        task.setSkipErrorEvent(config);
        return task;
    }
}
