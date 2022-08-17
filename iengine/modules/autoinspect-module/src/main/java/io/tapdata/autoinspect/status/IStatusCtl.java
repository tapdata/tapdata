package io.tapdata.autoinspect.status;

import com.tapdata.tm.autoinspect.constants.AutoInspectTaskStatus;
import com.tapdata.tm.autoinspect.constants.AutoInspectTaskType;

import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 21:06 Create
 */
public interface IStatusCtl {

    AutoInspectTaskStatus.Sync getSyncStatus();

    AutoInspectTaskType getSyncType();

    void waitExit(long timeouts) throws InterruptedException, TimeoutException;
}
