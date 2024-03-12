package io.tapdata.inspect.util;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.tm.commons.util.MetaType;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InspectJobUtil {

    public static TapAdvanceFilter wrapFilter(List<QueryOperator> srcConditions) {
        TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
        tapAdvanceFilter.setOperators(srcConditions);
        DataMap match = new DataMap();
        if (null != srcConditions) {
            srcConditions.stream()
                    .filter(op-> op.getOperator() == 5)
                    .forEach(op-> match.put(op.getKey(), op.getValue()));
        }
        tapAdvanceFilter.setMatch(match);
        return tapAdvanceFilter;
    }

    public static TapTable getTapTable(InspectDataSource inspectDataSource, InspectTaskContext inspectTaskContext) {
        Map<String, Object> params = new HashMap<>();
        params.put("connectionId", inspectDataSource.getConnectionId());
        params.put("metaType", MetaType.table.name());
        String table = inspectDataSource.getTable();
        params.put("tableName", table);
        TapTable tapTable = null == inspectTaskContext ?
                null :
                inspectTaskContext
                        .getClientMongoOperator()
                        .findOne(params, ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/metadata/v2", TapTable.class);
        if (null == tapTable) {
            tapTable = new TapTable(table);
        }
        return tapTable;
    }
}
