
package com.tapdata.tm.inspect.bean;


import com.tapdata.tm.commons.schema.Field;
import lombok.Data;

import java.util.List;

@Data
public class Source {

    /** */
    private List<Field> fields;
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
}
