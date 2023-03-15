package io.tapdata.zoho.service.zoho.schema;

import java.util.ArrayList;
import java.util.List;

public enum Schemas {
    Tickets("Tickets",         Tickets.class,        Boolean.TRUE),
    Departments("Departments", Departments.class,    Boolean.TRUE),
    Products("Products", Products.class,    Boolean.TRUE),
    OrganizationFields("OrganizationFields", OrganizationFields.class,    Boolean.TRUE),
    Contracts("Contracts",         Contracts.class,        Boolean.TRUE),
    Skills("Skills",         Skills.class,        Boolean.FALSE),
    Teams("Teams", Teams.class,    Boolean.FALSE),
    TicketComments("TicketComments", TicketComments.class,    Boolean.FALSE),
    TicketAttachments("TicketAttachments", TicketAttachments.class,    Boolean.FALSE),

    ;
    String tableName;
    Class<? extends Schema> schemaCls;
    boolean isUse;
    Schemas(String tableName,Class<? extends Schema> schemaCls,boolean isUse){
        this.schemaCls = schemaCls;
        this.tableName = tableName;
        this.isUse = isUse;
    }


    public static List<Schema> allSupportSchemas(){
        Schemas[] values = values();
        ArrayList<Schema> objects = new ArrayList<>();
        for (Schemas value : values) {
            if (null != value && value.isUse) {
                try {
                    objects.add(value.schemaCls.newInstance());
                } catch (InstantiationException e) {
                } catch (IllegalAccessException e) {
                }
            }
        }
        return objects.isEmpty()?null:objects;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Class getSchemaCls() {
        return schemaCls;
    }

    public void setSchemaCls(Class schemaCls) {
        this.schemaCls = schemaCls;
    }
}
