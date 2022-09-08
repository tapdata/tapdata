package com.tapdata.tm.autoinspect.compare;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import lombok.NonNull;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/5 18:50 Create
 */
public interface IQueryCompare {
    enum Status {
        Deleted, //Not in source and target
        FixTarget, //Fix by query target
        FixSource,//Fix by query source and target
        Diff, //Differences remain
        ;
    }

    /**
     * query source and target data by last difference results
     *
     * @param dto last difference results
     * @return status
     */
    Status queryCompare(@NonNull TaskAutoInspectResultDto dto);
}
