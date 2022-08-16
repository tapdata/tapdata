package io.tapdata.autoinspect.status;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 21:06 Create
 */
public interface ISyncStatusCtl extends IStatusCtl {

    void syncInitialing();

    void syncInitialized();

    void syncIncremental();

    void syncDone();

    void syncError(String msg);
}
