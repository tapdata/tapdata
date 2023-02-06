package com.tapdata.tm.init;

import lombok.NonNull;

import java.util.List;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 15:48 Create
 */
public interface IPatchScanner {

    /**
     * scan patch by function
     *
     * @param patches   patch list
     * @param isVersion check function
     */
    void scanPatches(@NonNull List<IPatch> patches, @NonNull Function<PatchVersion, Boolean> isVersion);
}
