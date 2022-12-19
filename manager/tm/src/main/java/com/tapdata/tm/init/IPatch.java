package com.tapdata.tm.init;

import lombok.NonNull;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 15:41 Create
 */
public interface IPatch extends Comparable<IPatch>, Runnable {

    PatchType type();

    PatchVersion version();

    @Override
    default int compareTo(@NonNull IPatch o) {
        return version().compareTo(o.version());
    }
}
