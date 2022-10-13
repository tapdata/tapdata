package io.tapdata.zoho.service.zoho.schema;

import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.table;
import static io.tapdata.entity.simplify.TapSimplify.field;

public class Tickets implements Schema {
    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table(Schemas.Tickets.getTableName())
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
    public List<TapTable> csv(List<String> tables, int tableSize) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentDocument(Map<String, Object> obj) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentCsv(Map<String, Object> obj) {
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj) {
        return null;
    }
}
