package io.tapdata.autoinspect.tester.samples;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.tester.AutoInspectTester;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 17:20 Create
 */
public class SampleAutoCompare implements IAutoCompare {
    private static final Logger logger = LogManager.getLogger(SampleAutoCompare.class);

    @Override
    public void add(@NonNull TaskAutoInspectResultDto taskAutoInspectResultDto) {
        Map<String, Object> sourceData = taskAutoInspectResultDto.getSourceData();
        if (null != sourceData && "fix".equals(sourceData.get("value"))) {
            logger.info("auto compare fix {}", AutoInspectTester.toJson(taskAutoInspectResultDto));
        } else {
            logger.info("save diff {}", AutoInspectTester.toJson(taskAutoInspectResultDto));
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("close auto compare");
    }
}
