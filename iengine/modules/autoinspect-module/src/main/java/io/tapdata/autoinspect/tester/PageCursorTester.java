package io.tapdata.autoinspect.tester;

import com.alibaba.fastjson.JSON;
import io.tapdata.autoinspect.connector.IDataCursor;
import io.tapdata.autoinspect.entity.CompareRecord;
import io.tapdata.autoinspect.connector.BatchDataCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PageCursorTester extends BatchDataCursor<CompareRecord> {
    private static final Logger logger = LogManager.getLogger(BatchDataCursor.class);

    public PageCursorTester(int offset, int limit, int counts) throws Exception {
        super(offset, limit);
        this.counts = counts;
    }

    private final int counts;

    @Override
    protected IDataCursor<CompareRecord> batchCursor(int offset, int limit) throws Exception {
        if (offset > counts) return null;

        logger.info("query page cursor: {}, {}", offset, limit);
        return new IDataCursor<CompareRecord>() {
            int currIndex = offset;
            final int endIdx = Math.min(counts, offset + limit);

            @Override
            public CompareRecord next() throws Exception {
                CompareRecord record = null;
                if (currIndex < endIdx) {
                    record = new CompareRecord();
                    record.getData().put("id", String.valueOf(currIndex++));
                }
                return record;
            }

            @Override
            public void close() throws Exception {

            }
        };
    }

    public static void main(String[] args) throws Exception {
        try (IDataCursor<CompareRecord> cursor = new PageCursorTester(0, 5, 18)) {
            CompareRecord record;
            while (null != (record = cursor.next())) {
                System.out.println(JSON.toJSONString(record.getData()));
            }
        }
    }
}
