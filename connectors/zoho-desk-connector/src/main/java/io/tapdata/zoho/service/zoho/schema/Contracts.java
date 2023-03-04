package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.ContractsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class Contracts implements Schema {
    /**
     * {
     *   "associatedSLAId" : "5000000007671",
     *   "modifiedTime" : "2018-01-15T11:13:04.000Z",
     *   "product" : {
     *     "id" : "500000001290",
     *     "productName" : "Desk"
     *   },
     *   "cf" : {
     *     "cf_mycustomfield1" : "hello"
     *   },
     *   "productId" : "5000000009328",
     *   "endDate" : "2019-01-10",
     *   "departmentId" : "5000000007235",
     *   "notifyBefore" : "5",
     *   "contractNumber" : "101",
     *   "description" : null,
     *   "sla" : {
     *     "name" : "Gold SLAs",
     *     "id" : "5000000007671"
     *   },
     *   "ownerId" : "5000000009148",
     *   "notificationAgentIds" : [ "5000000009148" ],
     *   "accountId" : "5000000009326",
     *   "createdBy" : "5000000009148",
     *   "notifyOn" : "2019-01-05",
     *   "createdTime" : "2018-01-10T11:29:41.000Z",
     *   "contractName" : "Ticket Resolution Contract for 5 Star Crisps",
     *   "modifiedBy" : "5000000009148",
     *   "id" : "5000000009331",
     *   "startDate" : "2018-01-10",
     *   "account" : {
     *     "website" : "https://www.zoho.com",
     *     "accountName" : "Zoho",
     *     "id" : "500000008909"
     *   }
     * }
     * */
    private ContractsOpenApi contractsOpenApi;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof ContractsOpenApi) this.contractsOpenApi = (ContractsOpenApi)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Contracts.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Contracts.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("associatedSLAId","StringMinor"))
                            .add(field("modifiedTime","StringMinor"))
                            .add(field("product","Map"))
                            .add(field("cf","Map"))
                            .add(field("productId","StringMinor"))
                            .add(field("endDate","StringMinor"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("notifyBefore","StringMinor"))
                            .add(field("contractNumber","StringMinor"))
                            .add(field("description","StringMinor"))
                            .add(field("sla","Map"))
                            .add(field("ownerId","StringMinor"))
                            .add(field("notificationAgentIds","Array"))
                            .add(field("accountId","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("notifyOn","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("contractName","StringMinor"))
                            .add(field("modifiedBy","StringMinor"))
                            .add(field("startDate","StringMinor"))
                            .add(field("account","Map"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Contracts.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("associatedSLAId","StringMinor"))
                            .add(field("modifiedTime","StringMinor"))
                            //.add(field("product","Map"))//@TODO
                            .add(field("cf","StringMinor"))// TODO
                            .add(field("productId","StringMinor"))
                            .add(field("endDate","StringMinor"))
                            .add(field("departmentId","StringMinor"))
                            .add(field("notifyBefore","StringMinor"))
                            .add(field("contractNumber","StringMinor"))
                            .add(field("description","StringMinor"))
                            .add(field("slaName","StringMinor"))
                            .add(field("slaId","StringMinor"))
                            .add(field("ownerId","StringMinor"))
                            .add(field("notificationAgentIds","StringMinor"))
                            .add(field("accountId","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("notifyOn","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("contractName","StringMinor"))
                            .add(field("modifiedBy","StringMinor"))
                            .add(field("startDate","StringMinor"))
                            .add(field("accountId","StringMinor"))
                            .add(field("accountWebsite","StringMinor"))
                            .add(field("accountAccountName","StringMinor"))
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
                "associatedSLAId",
                "modifiedTime",
                "product",
                "cf",
                "productId",
                "endDate",
                "departmentId",
                "notifyBefore",
                "contractNumber",
                "description",
                "sla.id",
                "sla.name",
                "ownerId",
                "notificationAgentIds",
                "accountId",
                "createdBy",
                "notifyOn",
                "createdTime",
                "contractName",
                "modifiedBy",
                "startDate",
                "account.id",
                "account.website",
                "account.accountName");
        MapUtil.valueToString(obj,"cf");
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> map, TapConnectionContext connectionContext) {
        if (Checker.isEmpty(map)) return Collections.emptyMap();
        Object contractIdObj = map.get("id");
        if (Checker.isEmpty(contractIdObj)) return map;
        if (Checker.isEmpty(this.contractsOpenApi)) this.contractsOpenApi = ContractsOpenApi.create(connectionContext);
        Map<String, Object> contract = this.contractsOpenApi.get(String.valueOf(contractIdObj));
        if (Checker.isEmpty(contract)) return map;
        return contract;
    }
}
