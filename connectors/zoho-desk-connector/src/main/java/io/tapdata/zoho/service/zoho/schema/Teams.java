package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.TeamsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class Teams implements Schema{
    /**
     * {
     *   "rolesWithSubordinates" : [ "6000000008684" ],
     *   "departmentId" : "6000000007245",
     *   "roles" : [ "6000000008686", "6000000011307" ],
     *   "name" : "Sales Representatives",
     *   "description" : "Sales teams for customer engagement.",
     *   "derivedAgents" : [ "6000000009086", "6000000012003" ],
     *   "id" : "6000000014005",
     *   "subTeams" : [ "6000000011305" ],
     *   "agents" : [ "6000000009086", "6000000012003" ]
     * }
     * */
    TeamsOpenApi teamsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof TeamsOpenApi) teamsOpenApi = (TeamsOpenApi)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Teams.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Teams.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("rolesWithSubordinates","Array"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("roles","Array"))
                            .add(field("name","StringMinor"))
                            .add(field("description","Textarea"))
                            .add(field("derivedAgents","Array"))
                            .add(field("subTeams","Array"))
                            .add(field("agents","Array"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Teams.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("rolesWithSubordinates","StringMinor"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("roles","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("description","Textarea"))
                            .add(field("derivedAgents","StringMinor"))
                            .add(field("subTeams","StringMinor"))
                            .add(field("agents","StringMinor"))
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
                "rolesWithSubordinates",
                "departmentId",
                "roles",
                "name",//id 列表，CSV格式使用|分割
                "description",
                "derivedAgents",
                "subTeams",
                "agents"
        );
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        if (Checker.isEmpty(map)) return map;
        Object teamIdObj = map.get("id");
        if (Checker.isEmpty(teamIdObj)) return map;
        if (Checker.isEmpty(this.teamsOpenApi)) this.teamsOpenApi = TeamsOpenApi.create(connectionContext);
        Map<String, Object> detailTeam = teamsOpenApi.get(String.valueOf(teamIdObj));
        return detailTeam;
    }
}
