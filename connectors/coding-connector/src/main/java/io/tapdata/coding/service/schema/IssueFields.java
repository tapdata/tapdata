package io.tapdata.coding.service.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class IssueFields implements SchemaStart {
    @Override
    public Boolean use() {
        return false;
    }

    @Override
    public String tableName() {
        return "IssueFields";
    }


    @Override
    public TapTable document(TapConnectionContext connectionContext) {
        return table(tableName())
                .add(field("IssueFieldId", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))
                .add(field("NeedDefault", JAVA_Boolean))
                .add(field("ValueString", "StringMinor"))
                .add(field("IssueType", "StringMinor"))
                .add(field("Required", JAVA_Boolean))
                .add(field("IssueField", JAVA_Map))
                .add(field("CreatedAt", JAVA_Long))
                .add(field("UpdatedAt", JAVA_Long));
    }

    @Override
    public boolean connection(TapConnectionContext tapConnectionContext) {
        return false;
    }
    /**
     * {
     *         "IssueFieldId": 1,
     *         "NeedDefault": false,
     *         "ValueString": "",
     *         "IssueType": "REQUIREMENT",
     *         "Required": false,
     *         "IssueField": {
     *           "Id": 1,
     *           "TeamId": 1,
     *           "Name": "处理人",
     *           "IconUrl": "",
     *           "Type": "ASSIGNEE",
     *           "ComponentType": "SELECT_MEMBER_SINGLE",
     *           "Description": "",
     *           "Options": [
     *
     *           ],
     *           "Unit": "",
     *           "Selectable": false,
     *           "Required": false,
     *           "Editable": false,
     *           "Deletable": false,
     *           "Sortable": true,
     *           "CreatedBy": 0,
     *           "CreatedAt": 1597283395000,
     *           "UpdatedAt": 1597283395000
     *         },
     *         "CreatedAt": 1597283400000,
     *         "UpdatedAt": 1597283400000
     *       }
     * */
}
