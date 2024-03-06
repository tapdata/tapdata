package io.tapdata.observable.logging.util.Conf;

import jnr.ffi.annotations.In;
import lombok.Data;

@Data
public class LogConfiguration {
    private Integer logSaveTime;
    private Integer logSaveSize;
    private Integer logSaveCount;

    public LogConfiguration(Integer logSaveTime, Integer logSaveSize, Integer logSaveCount) {
        this.logSaveTime = logSaveTime;
        this.logSaveSize = logSaveSize;
        this.logSaveCount = logSaveCount;
    }
}
