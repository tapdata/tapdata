package com.tapdata.tm.autoinspect.compare;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import lombok.NonNull;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 15:43 Create
 */
public interface IAutoCompare extends AutoCloseable {

    void add(@NonNull TaskAutoInspectResultDto taskAutoInspectResultDto);
}
