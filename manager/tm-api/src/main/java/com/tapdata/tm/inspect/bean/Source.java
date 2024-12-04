
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
    /** */
    private String direction;

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

}
