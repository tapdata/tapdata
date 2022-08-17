package io.tapdata.common.sample;

import io.tapdata.common.sample.request.BulkRequest;

/**
 * @author Dexter
 */
public  interface BulkReporter {
    void execute(BulkRequest bulkRequest);
}
