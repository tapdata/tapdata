package com.tapdata.tm.autoinspect.constants;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 12:27 Create
 */
public class TaskStatus {

    /**
     * sync status
     */
    public enum Sync {
        Scheduling, Initialing, Initialized, Incremental, Error, Done,
        ;

        public boolean in(Sync... statuses) {
            for (Sync status : statuses) {
                if (this == status) return true;
            }
            return false;
        }
    }

    /**
     * inspect status
     */
    public enum Inspect {
        Scheduling, WaitSyncInitialed, Initialing, WaitIncrementDelay, Incrementing, Stopping, Error, Done,
        ;

        public boolean in(Inspect... statuses) {
            for (Inspect status : statuses) {
                if (this == status) return true;
            }
            return false;
        }
    }
}
