package io.tapdata.dummy.constants;

/**
 * synchronization phase
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/8 17:21 Create
 */
public enum SyncStage {
    Initial, Incremental,
    ;

    public boolean isInitial() {
        return Initial == this;
    }

    public boolean isIncremental() {
        return Incremental == this;
    }

}
