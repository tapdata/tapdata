package com.tapdata.tm.init.patches;

import com.tapdata.tm.init.IPatch;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 16:54 Create
 */
public abstract class AbsPatch implements IPatch {
    protected PatchType type;
    protected PatchVersion version;

    public AbsPatch(PatchType type, PatchVersion version) {
        this.type = type;
        this.version = version;
    }

    @Override
    public PatchType type() {
        return type;
    }

    @Override
    public PatchVersion version() {
        return version;
    }
}
