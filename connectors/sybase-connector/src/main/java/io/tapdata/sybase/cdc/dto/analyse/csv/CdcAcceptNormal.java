package io.tapdata.sybase.cdc.dto.analyse.csv;

import java.util.List;

public interface CdcAcceptNormal<T> extends CdcAccepter {
    public default void acceptObject(List<T> compileLines){}

}
