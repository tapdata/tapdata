package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.TicketAttachmentsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

/**
 * 工单附件表
 * */
public class TicketAttachments implements Schema{
    /**
     * {
     *   "creator" : {
     *     "firstName" : "Jade",
     *     "lastName" : "Tywin",
     *     "photoURL" : "https://desk.zoho.com.cn/api/v1/agents/1892000000047001/photo?orgId=298902",
     *     "id" : "1892000000047001",
     *     "email" : "jade@zylker.com"
     *   },
     *   "size" : "1079",
     *   "creatorId" : "1892000000047001",
     *   "name" : "ScreenShot",
     *   "createdTime" : "2013-11-06T10:25:03.000Z",
     *   "isPublic" : true,
     *   "id" : "1892000000047041",
     *   "href" : "https://desk.zoho.com.cn/api/v1/tickets/1892000001004024/attachments/1892000000047041/content"
     * }
     * */
    private TicketAttachmentsOpenApi attachmentsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof TicketAttachmentsOpenApi) attachmentsOpenApi = (TicketAttachmentsOpenApi) openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.TicketAttachments.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.TicketAttachments.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("href","StringMinor"))
                            .add(field("isPublic","Boolean"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("creatorId","StringMinor"))
                            .add(field("size","Integer"))
                            .add(field("creator","Map"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.TicketAttachments.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("href","StringMinor"))
                            .add(field("isPublic","Boolean"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("name","StringMinor"))
                            .add(field("creatorId","StringMinor"))
                            .add(field("size","Integer"))
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
                "href",
                "isPublic",
                "createdTime",
                "name",//id 列表，CSV格式使用|分割
                "creatorId",
                "size"
        );
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        return map;
    }
}
