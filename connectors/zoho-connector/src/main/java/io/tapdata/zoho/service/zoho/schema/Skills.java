package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.SkillsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class Skills implements Schema{
    /**
     * {
     *   "modifiedTime" : "2020-07-23T13:03:12.994Z",
     *   "agentIds" : [ "1000000000059" ],
     *   "skillType" : {
     *     "skillTypeId" : "1000000173001",
     *     "skillTypeName" : "Country"
     *   },
     *   "orderId" : "2",
     *   "criteria" : {
     *     "fieldConditions" : [ {
     *       "displayValue" : [ "Russia" ],
     *       "condition" : "is",
     *       "fieldName" : "Subject",
     *       "fieldModule" : "tickets",
     *       "value" : [ "Russia" ]
     *     }, {
     *       "displayValue" : [ "987532114" ],
     *       "condition" : "is",
     *       "fieldName" : "Phone",
     *       "fieldModule" : "tickets",
     *       "value" : [ "987532114" ]
     *     } ],
     *     "pattern" : "(1or2)"
     *   },
     *   "departmentId" : "1000000013248",
     *   "description" : "Country is Russia",
     *   "skillTypeId" : "1000000173001",
     *   "createdBy" : "1000000000059",
     *   "name" : "Russia",
     *   "createdTime" : "2020-07-23T13:03:12.994Z",
     *   "modifiedBy" : "1000000000059",
     *   "id" : "1000000173047",
     *   "status" : "ACTIVE"
     * }
     * */
    private SkillsOpenApi skillsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof SkillsOpenApi) this.skillsOpenApi = (SkillsOpenApi)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Skills.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Skills.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("orderId","StringMinor"))
                            .add(field("criteria","Array"))
                            .add(field("description","Textarea"))
                            .add(field("skillType","Map"))
                            .add(field("agentIds","Array"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("skillTypeId","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("modifiedBy","StringMinor"))
                            .add(field("modifiedTime","StringMinor"))
                            .add(field("status","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Skills.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("orderId","StringMinor"))
                            .add(field("criteria","StringMinor"))//@TODO
                            .add(field("description","Textarea"))
                            //.add(field("skillType","Map"))
                            .add(field("agentIds","StringMinor"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("skillTypeId","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("modifiedBy","StringMinor"))
                            .add(field("modifiedTime","StringMinor"))
                            .add(field("status","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        Map<String,Object> cSVDetail = new HashMap<>();
        //统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(obj, cSVDetail,
                "id",
                "orderId",
                "criteria",
                "description",
                "agentIds",//id 列表，CSV格式使用|分割
                "departmentId",
                "skillTypeId",
                "createdBy",
                "name",
                "createdTime",
                "modifiedBy",
                "modifiedTime",
                "status"
        );
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        return map;
    }
}
