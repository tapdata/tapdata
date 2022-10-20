package io.tapdata.zoho.service.zoho.webHook;

import io.tapdata.zoho.service.zoho.schema.Schemas;

public enum  WebHookEvent {
    Ticket_Add("Ticket_Add",                                "AddEvent",    Schemas.Tickets.getTableName()),
    Ticket_Update("Ticket_Update",                          "UpdateEvent", Schemas.Tickets.getTableName()),
    Ticket_Delete("Ticket_Delete",                          "DeleteEvent", Schemas.Tickets.getTableName()),
    Ticket_Approval_Add("Ticket_Approval_Add",              "AddEvent",    Schemas.Tickets.getTableName()),
    Ticket_Approval_Update("Ticket_Approval_Update",        "UpdateEvent", Schemas.Tickets.getTableName()),
    Ticket_Thread_Add("Ticket_Thread_Add",                  "AddEvent",    Schemas.Tickets.getTableName()),
    Ticket_Comment_Add("Ticket_Comment_Add",                "AddEvent",    Schemas.TicketComments.getTableName()),
    Ticket_Comment_Update("Ticket_Comment_Update",          "UpdateEvent", Schemas.TicketComments.getTableName()),
    Contact_Add("Contact_Add",                              "AddEvent",    Schemas.Contracts.getTableName()),
    Contact_Update("Contact_Update",                        "UpdateEvent", Schemas.Contracts.getTableName()),
    Contact_Delete("Contact_Delete",                        "DeleteEvent", Schemas.Contracts.getTableName()),
    Account_Add("Account_Add",                              "AddEvent",   ""),
    Account_Update("Account_Update",                        "UpdateEvent",""),
    Account_Delete("Account_Delete",                        "DeleteEvent",""),
    Department_Add("Department_Add",                        "AddEvent",    Schemas.Departments.getTableName()),
    Department_Update("Department_Update",                  "UpdateEvent", Schemas.Departments.getTableName()),
    Agent_Add("Agent_Add",                                  "AddEvent",   ""),
    Agent_Update("Agent_Update",                            "UpdateEvent",""),
    Agent_Delete("Agent_Delete",                            "DeleteEvent",""),
    Agent_Presence_Update("Agent_Presence_Update",          "UpdateEvent",""),
    Ticket_Attachment_Add("Ticket_Attachment_Add",          "AddEvent",    Schemas.TicketAttachments.getTableName()),
    Ticket_Attachment_Update("Ticket_Attachment_Update",    "UpdateEvent", Schemas.TicketAttachments.getTableName()),
    Ticket_Attachment_Delete("Ticket_Attachment_Delete",    "DeleteEvent", Schemas.TicketAttachments.getTableName()),
    Task_Add("Task_Add",                                    "AddEvent",   ""),
    Task_Update("Task_Update",                              "UpdateEvent",""),
    Task_Delete("Task_Delete",                              "DeleteEvent",""),
    Call_Add("Call_Add",                                    "AddEvent",   ""),
    Call_Update("Call_Update",                              "UpdateEvent",""),
    Call_Delete("Call_Delete",                              "DeleteEvent",""),
    Event_Add("Event_Add",                                  "AddEvent",   ""),
    Event_Update("Event_Update",                            "UpdateEvent",""),
    Event_Delete("Event_Delete",                            "DeleteEvent",""),
    TimeEntry_Add("TimeEntry_Add",                          "AddEvent",   ""),
    TimeEntry_Update("TimeEntry_Update",                    "UpdateEvent",""),
    TimeEntry_Delete("TimeEntry_Delete",                    "DeleteEvent",""),
    Article_Add("Article_Add",                              "AddEvent",   ""),
    Article_Update("Article_Update",                        "UpdateEvent",""),
    Article_Delete("Article_Delete",                        "DeleteEvent",""),
    Article_Translation_Add("Article_Translation_Add",      "AddEvent",   ""),
    Article_Translation_Update("Article_Translation_Update","UpdateEvent",""),
    Article_Translation_Delete("Article_Translation_Delete","DeleteEvent",""),
    Article_Feedback_Add("Article_Feedback_Add",            "AddEvent",   ""),
    KB_RootCategory_Add("KB_RootCategory_Add",              "AddEvent",   ""),
    KB_RootCategory_Update("KB_RootCategory_Update",        "UpdateEvent",""),
    KB_RootCategory_Delete("KB_RootCategory_Delete",        "DeleteEvent",""),
    KB_Section_Add("KB_Section_Add",                        "AddEvent",   ""),
    KB_Section_Update("KB_Section_Update",                  "UpdateEvent",""),
    KB_Section_Delete("KB_Section_Delete",                  "DeleteEvent",""),

    ;
    String type;
    String tapEvent;
    String eventTable;
    WebHookEvent(String type,String tapEvent,String eventTable){
        this.type = type;
        this.tapEvent = tapEvent;
        this.eventTable = eventTable;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTapEvent() {
        return tapEvent;
    }

    public void setTapEvent(String tapEvent) {
        this.tapEvent = tapEvent;
    }
    public static WebHookEvent event(String type){
        if (null == type || "".equals(type)) return null;
        WebHookEvent[] values = values();
        for (WebHookEvent value : values) {
            if (value.type.equals(type)) return value;
        }
        return null;
    }

    public String getEventTable() {
        return eventTable;
    }

    public void setEventTable(String eventTable) {
        this.eventTable = eventTable;
    }
}
