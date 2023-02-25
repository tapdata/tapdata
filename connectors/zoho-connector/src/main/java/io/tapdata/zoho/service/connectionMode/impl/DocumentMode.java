package io.tapdata.zoho.service.connectionMode.impl;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class DocumentMode implements ConnectionMode {
    private static final String TAG = DocumentMode.class.getSimpleName();
    TapConnectionContext connectionContext;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        return this;
    }
    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize ) {
        List<Schema> schemas = Schemas.allSupportSchemas();
        if (null != schemas && !schemas.isEmpty()){
            List<TapTable> tapTables = new ArrayList<>();
            schemas.forEach(schema -> tapTables.addAll(schema.document(tables,tableSize)));
            return null != tapTables && !tapTables.isEmpty()? tapTables : null;
        }
        return null;
    }
    @Override
    public List<TapTable> discoverSchemaV1(List<String> tables, int tableSize ) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table("Tickets")
                            .add(field("id","Long").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("modifiedTime","DateTime"))
                            .add(field("description","Textarea"))
                            .add(field("subCategory","Map"))//Object
                            .add(field("statusType","StringMinor"))
                            .add(field("subject","StringNormal"))
                            .add(field("dueDate","DateTime"))
                            .add(field("departmentId","Long"))
                            .add(field("channel","StringMinor"))
                            .add(field("onholdTime","DateTime"))//Object
                            .add(field("language","StringMinor"))
                            .add(field("source","Map"))
                            .add(field("resolution","StringNormal"))//Object
                            .add(field("sharedDepartments","JAVA_Array"))
                            .add(field("closedTime","DateTime"))//Object
                            .add(field("approvalCount","Integer"))
                            .add(field("isOverDue","Boolean"))//Object
                            .add(field("isTrashed","Boolean"))//Boolean
                            .add(field("contact","Map"))
                            .add(field("createdTime","DateTime"))
                            .add(field("isResponseOverdue","Boolean"))//Object
                            .add(field("customerResponseTime","DateTime"))
                            .add(field("productId","Long"))//Object
                            .add(field("contactId","Long"))
                            .add(field("threadCount","Integer"))
                            .add(field("secondaryContacts","JAVA_Array"))
                            .add(field("priority","StringMinor"))
                            .add(field("classification","StringNormal"))
                            .add(field("commentCount","Integer"))
                            .add(field("taskCount","Integer"))
                            .add(field("accountId","Long"))//Object
                            .add(field("phone","Phone"))
                            .add(field("webUrl","URL"))
                            .add(field("assignee","Map"))
                            .add(field("isSpam","Boolean"))//Object
                            .add(field("status","StringMinor"))
                            .add(field("entitySkills","JAVA_Array"))
                            .add(field("ticketNumber","Integer"))
                            .add(field("sentiment","Map"))//Object
                            .add(field("customFields","Map"))
                            .add(field("isArchived","Boolean"))//Object
                            .add(field("Textarea","Textarea"))
                            .add(field("timeEntryCount","Integer"))
                            .add(field("channelRelatedInfo","Map"))//Object
                            .add(field("responseDueDate","Date"))//Object
                            .add(field("isDeleted","Boolean"))//Object
                            .add(field("modifiedBy","Long"))
                            .add(field("department","Map"))
                            .add(field("followerCount","Integer"))
                            .add(field("email","Email"))
                            .add(field("layoutDetails","Map"))
                            .add(field("channelCode","StringMinor"))//Object
                            .add(field("product","Map"))//Object
                            .add(field("isFollowing","Boolean"))//Object
                            .add(field("cf","Map"))
                            .add(field("slaId","Long"))
                            .add(field("team","Map"))//Object
                            .add(field("layoutId","Long"))
                            .add(field("assigneeId","Long"))
                            .add(field("createdBy","Long"))
                            .add(field("teamId","Long"))//Object
                            .add(field("tagCount","Integer"))
                            .add(field("attachmentCount","Integer"))
                            .add(field("isEscalated","Boolean"))//Object
                            .add(field("category","Map"))//Object
                            .add(field("department","Map"))
                            .add(field("contact","Map"))//Object
                            .add(field("assignee","Map"))//Object
            );
        }
        return null;
    }

    @Override
    public Map<String,Object> attributeAssignmentSelf(Map<String,Object> ticketDetail,String tableName) {
        this.removeJsonNull(ticketDetail);
        return ticketDetail;
    }

    public Map<String,Object> attributeAssignmentSelfV2(Map<String,Object> obj,String tableName) {
        return Schema.schema(tableName).attributeAssignmentSelfDocument(obj);
    }
    public Map<String,Object> attributeAssignmentV2(Map<String,Object> stringObjectMap,String tableName) {
        Object ticketIdObj = stringObjectMap.get("id");
        if (Checker.isEmpty(ticketIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        Map<String, Object> ticketDetail = TicketLoader.create(connectionContext).getOne((String) ticketIdObj);
        ticketDetail.put("department",stringObjectMap.get("department"));
        ticketDetail.put("contact",stringObjectMap.get("contact"));
        ticketDetail.put("assignee",stringObjectMap.get("assignee"));
        return this.attributeAssignmentSelf(ticketDetail,tableName);
    }

    @Override
    public Map<String,Object> attributeAssignment(Map<String,Object> stringObjectMap,String tableName, ZoHoBase openApi) {
        return Schema.schema(tableName).config(openApi).attributeAssignmentDocument(stringObjectMap,connectionContext);
    }
}
