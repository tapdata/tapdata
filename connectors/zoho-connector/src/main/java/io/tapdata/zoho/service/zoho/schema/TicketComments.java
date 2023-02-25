package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.TicketCommentsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class TicketComments implements Schema{
    /**
     * {
     *   "commentedTime" : "2014-11-28T10:25:13.000Z",
     *   "isPublic" : false,
     *   "id" : "1892000000366001",
     *   "contentType" : "html",
     *   "content" : "Sample zsu[@user:55616589]zsu and zsu[@team:31138000001254025_new team]zsu testing",
     *   "commenterId" : "1892000000042001",
     *   "mention" : [ {
     *     "firstName" : "",
     *     "lastName" : "pandees",
     *     "photoURL" : null,
     *     "offSet" : "7",
     *     "length" : "28",
     *     "id" : "31138000000573164",
     *     "type" : "AGENT",
     *     "email" : "carol@zylker.com",
     *     "zuid" : "55616589"
     *   }, {
     *     "offSet" : "39",
     *     "length" : "46",
     *     "name" : "new team",
     *     "id" : "31138000001254025",
     *     "type" : "TEAM"
     *   }, {
     *     "offSet" : "59",
     *     "departmentId" : "3113800000634345",
     *     "entityNumber" : "1342",
     *     "length" : "26",
     *     "name" : "",
     *     "id" : "3113800000143134",
     *     "type" : "TICKET"
     *   } ],
     *   "commenter" : {
     *     "firstName" : "Jade",
     *     "lastName" : "Tywin",
     *     "photoURL" : "https://desk.zoho.com.cn/api/v1/portalUser/4000000008692/photo?orgId=292828",
     *     "name" : "Jade Tywin",
     *     "roleName" : "PortalUser",
     *     "type" : "END_USER",
     *     "email" : "jade12tywin@zylker.com"
     *   }
     * }
     * */
    private TicketCommentsOpenApi commentsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof TicketCommentsOpenApi) this.commentsOpenApi = (TicketCommentsOpenApi)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.TicketComments.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.TicketComments.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("contentType","StringStatus"))
                            .add(field("commenterId","StringMinor"))
                            .add(field("content","StringMinor"))
                            .add(field("mention","Array"))
                            .add(field("commenter","Map"))
                            .add(field("isPublic","Boolean"))
                            .add(field("commentedTime","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.TicketComments.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("contentType","StringStatus"))
                            .add(field("commenterId","StringMinor"))
                            .add(field("content","StringMinor"))
                            .add(field("mentionId","StringMinor"))
                            .add(field("isPublic","Boolean"))
                            .add(field("commentedTime","StringMinor"))
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
                "contentType",
                "commenterId",
                "content",
                "mention.id",//id 列表，CSV格式使用|分割
                "isPublic",
                "commentedTime"
        );
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        return map;
    }
}
