package com.tapdata.tm.metaData.param;

import lombok.Data;

@Data
public class AddMetadataInstanceParam{

/*    alias_name: ""
    comment: ""
    connectionId: "6113498c89443123b98b9636"
    create_source: "manual"
    databaseId: "6113498c89443123b98b9636"
    is_deleted: false
    meta_type: "collection"
    original_name: "ldw_test"*/

    private String alias_name;
    private String comment;
    private String connectionId;
    private String create_source;
    private String databaseId;
    private String meta_type;
    private String original_name;

}
