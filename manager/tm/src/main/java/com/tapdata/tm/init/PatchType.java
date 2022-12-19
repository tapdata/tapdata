package com.tapdata.tm.init;

import com.tapdata.tm.sdk.util.AppType;
import lombok.NonNull;

import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 17:39 Create
 */
public class PatchType {

    private final @NonNull AppType appType;
    private final @NonNull PatchTypeEnums type;

    public PatchType(@NonNull AppType appType, @NonNull PatchTypeEnums type) {
        this.appType = appType;
        this.type = type;
    }

    public AppType getAppType() {
        return appType;
    }

    public PatchTypeEnums getType() {
        return type;
    }

    public boolean inAppTypes(@NonNull AppType... appTypes) {
        for (AppType t : appTypes) {
            if (t == appType) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s_%s_version", appType, type).toLowerCase();
    }

}
