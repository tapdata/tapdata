package io.tapdata.modules.api.net.data;

import io.tapdata.entity.error.CoreException;
import io.tapdata.modules.api.net.error.NetErrors;

public class Ping extends Data {
    public final static byte TYPE = 111;

    public Ping(){
        super(TYPE);
    }


}