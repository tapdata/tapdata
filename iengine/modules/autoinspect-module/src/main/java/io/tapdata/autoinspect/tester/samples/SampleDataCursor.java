package io.tapdata.autoinspect.tester.samples;

import io.tapdata.autoinspect.connector.IDataCursor;
import io.tapdata.autoinspect.entity.CompareRecord;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 16:11 Create
 */
public class SampleDataCursor implements IDataCursor<CompareRecord> {
    private static final Logger logger = LogManager.getLogger(SampleDataCursor.class);
    private int index = 0;
    private final @NonNull List<CompareRecord> recordList;

    public SampleDataCursor(@NonNull List<CompareRecord> recordList) {
        this.recordList = recordList;
    }

    @Override
    public CompareRecord next() throws Exception {
        CompareRecord record = null;
        if (index < recordList.size()) {
            record = new CompareRecord();
            record.copyFrom(recordList.get(index++));
        }
        return record;
    }

    @Override
    public void close() throws Exception {
        logger.info("close");
    }
}
