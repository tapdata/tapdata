package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class OrganizationFields implements Schema {
    private OrganizationFieldsOpenApi fieldsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof OrganizationFieldsOpenApi) this.fieldsOpenApi = (OrganizationFieldsOpenApi)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.OrganizationFields.getTableName();
    }

    /**
     *  "displayLabel" : "Account Name",
     *     "apiName" : "accountId",
     *     "isCustomField" : false,
     *     "showToHelpCenter" : true,
     *     "isEncryptedField" : false,
     *     "id" : "4000000000355",
     *     "type" : "LookUp",
     *     "maxLength" : 300,
     *     "isMandatory" : fals
     *     "allowedValues":[{}]
     *   "toolTipType" : "placeHolder",
     *   "toolTip" : "Sample Text Field",
     * */
    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.OrganizationFields.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("displayLabel","StringMinor"))
                            .add(field("apiName","StringMinor"))
                            .add(field("showToHelpCenter","Boolean"))
                            .add(field("isCustomField","Boolean"))
                            .add(field("isEncryptedField","Boolean"))
                            .add(field("maxLength","Integer"))
                            .add(field("type","StringMinor"))
                            .add(field("isMandatory","Boolean"))
                            .add(field("allowedValues","Array"))
                            .add(field("toolTipType","StringMinor"))
                            .add(field("toolTip","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.OrganizationFields.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("displayLabel","StringMinor"))
                            .add(field("apiName","StringMinor"))
                            .add(field("showToHelpCenter","Boolean"))
                            .add(field("isCustomField","Boolean"))
                            .add(field("isEncryptedField","Boolean"))
                            .add(field("maxLength","Integer"))
                            .add(field("type","StringMinor"))
                            .add(field("isMandatory","Boolean"))
                            .add(field("allowedValues","StringMinor"))
                            .add(field("toolTipType","StringMinor"))
                            .add(field("toolTip","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        return map;
    }


    //    @Override
//    public Map<String, Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext) {
//        return this.attributeAssignmentSelfDocument(obj);
//    }
//
//    @Override
//    public Map<String, Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig) {
//        return this.attributeAssignmentSelfCsv(obj,contextConfig);
//    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        Map<String,Object> ticketCSVDetail = new HashMap<>();
        //统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(obj, ticketCSVDetail,
                "id",    //"Ticket Reference Id"
                "displayLabel",
                "apiName",
                "showToHelpCenter",
                "isCustomField",
                "isEncryptedField",
                "maxLength",
                "type",
                "isMandatory",
                "toolTipType",
                "toolTip"
        );
        MapUtil.valueToString(ticketCSVDetail,"allowedValues");
        this.removeJsonNull(ticketCSVDetail);
        return ticketCSVDetail;
    }
}
