package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.service.zoho.loader.ProductsOpenApi;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;
import static io.tapdata.base.ConnectorBase.field;

public class Products implements Schema {
    private static final String TAG = Products.class.getSimpleName();

    private ProductsOpenApi productsOpenApi;
    public Products config(ZoHoBase openApi){
        if (Checker.isNotEmpty(openApi) && openApi instanceof ProductsOpenApi )
            this.productsOpenApi = (ProductsOpenApi) openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Products.getTableName();
    }

    /**
     * "owner" : {
     *     "photoURL" : "https://desk.zoho.com.cn/api/v1/agents/6000000009050/photo?orgId=581861259",
     *     "firstName" : "Aaron",
     *     "lastName" : "Stone",
     *     "name" : "Aaron Stone",
     *     "id" : "6000000009050"
     *   },
     *   "unitPrice" : "100.0",
     *   "modifiedTime" : "2017-06-28T13:25:06.000Z",
     *   "cf" : {
     *   },
     *   "description" : null,
     *   "departmentIds" : [ "6000000009450", "6000000009380", "6000000009230" ],
     *   "ownerId" : "6000000009050",
     *   "layoutId" : "6000000002556",
     *   "productName" : "Dell",
     *   "productCategory" : null,
     *   "productCode" : "12345",
     *   "isDeleted" : false,
     *   "createdBy" : "6000000009050",
     *   "createdTime" : "2017-06-28T13:25:06.000Z",
     *   "modifiedBy" : "6000000009050",
     *   "id" : "6000000124009",
     *   "departments" : [ {
     *     "name" : "Associated Department 1",
     *     "id" : "6000000009450"
     *   }, {
     *     "name" : "Associated Department 2",
     *     "id" : "6000000009380"
     *   }, {
     *     "name" : "Associated Department 3",
     *     "id" : "6000000009230"
     *   } ]
     * */
    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Products.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("departments","Array"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("isDeleted","Boolean"))
                            .add(field("productCode","StringMinor"))
                            .add(field("productCategory","StringMinor"))
                            .add(field("productName","StringMinor"))
                            .add(field("ownerId","StringMinor"))
                            .add(field("layoutId","StringMinor"))
                            .add(field("departmentIds","Array"))
                            .add(field("description","Textarea"))
                            .add(field("cf","Map"))
                            .add(field("modifiedTime","StringMinor"))
                            .add(field("unitPrice","price"))
                            .add(field("ownerId","StringMinor"))
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Products.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("departmentIds","StringMinor"))
                            .add(field("createdTime","StringMinor"))
                            .add(field("createdBy","StringMinor"))
                            .add(field("isDeleted","Boolean"))
                            .add(field("productCode","StringMinor"))
                            .add(field("productCategory","StringMinor"))
                            .add(field("productName","StringMinor"))
                            .add(field("ownerId","StringMinor"))
                            .add(field("layoutId","StringMinor"))
                            .add(field("description","Textarea"))
                            .add(field("cf","Map"))
                            .add(field("modifiedTime","StringMinor"))
                            .add(field("unitPrice","price"))
            );
        }
        return null;
    }

//    @Override
//    public Map<String, Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext) {
//        if (Checker.isEmpty(productsOpenApi)){
//            productsOpenApi = ProductsOpenApi.create(connectionContext);
//        }
//        Object productIdObj = obj.get("id");
//        if (Checker.isEmpty(productIdObj)){
//            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
//            return Collections.emptyMap();
//        }
//        Map<String,Object> product = productsOpenApi.get(String.valueOf(productIdObj));
//        return this.attributeAssignmentSelfDocument(product);
//    }

//    @Override
//    public Map<String, Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig) {
//        if (Checker.isEmpty(productsOpenApi)){
//            productsOpenApi = ProductsOpenApi.create(connectionContext);
//        }
//        Object productIdObj = obj.get("id");
//        if (Checker.isEmpty(productIdObj)){
//            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
//            return Collections.emptyMap();
//        }
//        Map<String,Object> product = productsOpenApi.get(String.valueOf(productIdObj));
//        return this.attributeAssignmentSelfCsv(product,contextConfig);
//    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        Map<String,Object> cSVDetail = new HashMap<>();
        //统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(obj, cSVDetail,
                "id",
                "createdTime",
                "createdBy",
                "isDeleted",
                "productCode",
                "productCategory",
                "productName",
                "ownerId",
                "layoutId",
                "departmentIds",
                "description",
                "cf",//@TODO "Map",
                "modifiedTime",
                "unitPrice"
        );
        this.removeJsonNull(cSVDetail);
        return cSVDetail;
    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> obj, TapConnectionContext connectionContext) {
        if (Checker.isEmpty(productsOpenApi)){
            productsOpenApi = ProductsOpenApi.create(connectionContext);
        }
        Object productIdObj = obj.get("id");
        if (Checker.isEmpty(productIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
            return Collections.emptyMap();
        }
        return productsOpenApi.get(String.valueOf(productIdObj));
    }
}
