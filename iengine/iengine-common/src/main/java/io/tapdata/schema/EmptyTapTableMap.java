package io.tapdata.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

public class EmptyTapTableMap implements KVReadOnlyMap<TapTable>{

    @Override
    public TapTable get(String s) {
        return null;
    }
}
