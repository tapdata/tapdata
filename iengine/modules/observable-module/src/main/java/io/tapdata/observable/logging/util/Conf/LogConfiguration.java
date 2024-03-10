package io.tapdata.observable.logging.util.Conf;


import com.tapdata.entity.Setting;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class LogConfiguration {
    private Integer logSaveTime;
    private Integer logSaveSize;
    private Integer logSaveCount;
    private String logLevel;
    private String scriptEngineHttpAppender;

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public void setScriptEngineHttpAppender(String scriptEngineHttpAppender) {
        this.scriptEngineHttpAppender = scriptEngineHttpAppender;
    }
}
