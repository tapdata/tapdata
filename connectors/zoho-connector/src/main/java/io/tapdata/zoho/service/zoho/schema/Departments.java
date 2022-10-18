package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.DepartmentOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;

/**
 * 部门表 ，Schema 的 部门策略
 * */
public class Departments implements Schema {
    private DepartmentOpenApi departmentOpenApi;

    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof DepartmentOpenApi) this.departmentOpenApi = (DepartmentOpenApi)departmentOpenApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Departments.getTableName();
    }

    /**
     * {
     *   "isAssignToTeamEnabled" : true,
     *   "chatStatus" : "AVAILABLE",
     *   "hasLogo" : true,
     *   "isVisibleInCustomerPortal" : true,
     *   "creatorId" : "1892000000042001",
     *   "description" : "Zylker Inc. is a multinational technology company that designs, develops, and sells consumer electronics.",
     *   "associatedAgentIds" : [ "1892000000042001", "1892000000056007", "1892000000888059" ],
     *   "isDefault" : true,
     *   "isEnabled" : true,
     *   "name" : "Zylker",
     *   "createdTime" : "2019-07-26T13:11:02.000Z",
     *   "id" : "1892000000082069",
     *   "nameInCustomerPortal" : "ZylCares"
     * }
     * */
    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Departments.getTableName())
                    .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                    .add(field("nameInCustomerPortal","StringMinor"))
                    .add(field("createdTime","StringMinor"))
                    .add(field("name","StringMinor"))
                    .add(field("isEnabled","Boolean"))
                    .add(field("isDefault","Boolean"))
                    .add(field("associatedAgentIds","Array"))
                    .add(field("description","Textarea"))
                    .add(field("creatorId","StringMinor"))
                    .add(field("isVisibleInCustomerPortal","Boolean"))
                    .add(field("hasLogo","Boolean"))
                    .add(field("chatStatus","StringStatus"))
                    .add(field("isAssignToTeamEnabled","Boolean"))
            );
        }
        return null;
    }

    /**
     * Departments list 返回的结果已经是详情了，不需要在去获取详情
     * */
    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Departments.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("nameInCustomerPortal","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("isEnabled","Boolean"))
                            .add(field("isDefault","Boolean"))
                            .add(field("associatedAgentIds","Textarea"))//CSV格式，数组用|分割的字符串
                            .add(field("description","Textarea"))
                            .add(field("creatorId","StringMinor"))
                            .add(field("isVisibleInCustomerPortal","Boolean"))
                            .add(field("hasLogo","Boolean"))
                            .add(field("chatStatus","StringStatus"))
                            .add(field("isAssignToTeamEnabled","Boolean"))
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

    /**
     * Departments list 返回的结果已经是详情了，不需要在去获取详情
     * */
    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        Map<String,Object> ticketCSVDetail = new HashMap<>();
        //统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(obj, ticketCSVDetail,
                "id",    //"Ticket Reference Id"
                "nameInCustomerPortal",
                "createdTime",
                "name",
                "isEnabled",
                "isDefault",
                "associatedAgentIds",//CSV格式，数组用|分割的字符串
                "description",
                "creatorId",
                "isVisibleInCustomerPortal",
                "hasLogo",
                "chatStatus",
                "isAssignToTeamEnabled"
        );
        this.removeJsonNull(ticketCSVDetail);
        return ticketCSVDetail;
    }
}
