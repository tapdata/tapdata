
package com.tapdata.tm.inspect.bean;


import com.tapdata.tm.commons.schema.Field;
import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Source {

    /** */
    private List<Field> fields;
    private List<String> columns;
    /** */
    private String connectionId;
    /** */
    private String connectionName;
    /** SQL used to produce the result set for custom_query inspections. */
    private String customQuery;
    /** */
    private String direction;

    /** Legacy table-inspection sort column; custom_query ordering is configured on Task.sortFields. */
    private String sortColumn;
    /** */
    private String table;
    private String nodeId;
    private String nodeName;
    private String databaseType;
    private Boolean isFilter;
    List<QueryOperator> conditions;
    private boolean enableCustomCommand;
    private Map<String, Object> customCommand;
    private boolean enableCustomCollate;
    private Map<String, String> collate;
    private int customNullSort;
}
