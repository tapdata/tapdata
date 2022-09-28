package io.tapdata.zoho.enums;

public enum  WebHookEvent {
    Ticket_Add("Ticket_Add",                                "ADD"),
    Ticket_Update("Ticket_Update",                          "UPDATE"),
    Ticket_Delete("Ticket_Delete",                          "DELETE"),
    Ticket_Approval_Add("Ticket_Approval_Add",              "ADD"),
    Ticket_Approval_Update("Ticket_Approval_Update",        "UPDATE"),
    Ticket_Thread_Add("Ticket_Thread_Add",                  "ADD"),
    Ticket_Comment_Add("Ticket_Comment_Add",                "ADD"),
    Ticket_Comment_Update("Ticket_Comment_Update",          "UPDATE"),
    Contact_Add("Contact_Add",                              "ADD"),
    Contact_Update("Contact_Update",                        "UPDATE"),
    Contact_Delete("Contact_Delete",                        "DELETE"),
    Account_Add("Account_Add",                              "ADD"),
    Account_Update("Account_Update",                        "UPDATE"),
    Account_Delete("Account_Delete",                        "DELETE"),
    Department_Add("Department_Add",                        "ADD"),
    Department_Update("Department_Update",                  "UPDATE"),
    Agent_Add("Agent_Add",                                  "ADD"),
    Agent_Update("Agent_Update",                            "UPDATE"),
    Agent_Delete("Agent_Delete",                            "DELETE"),
    Agent_Presence_Update("Agent_Presence_Update",          "UPDATE"),
    Ticket_Attachment_Add("Ticket_Attachment_Add",          "ADD"),
    Ticket_Attachment_Update("Ticket_Attachment_Update",    "UPDATE"),
    Ticket_Attachment_Delete("Ticket_Attachment_Delete",    "DELETE"),
    Task_Add("Task_Add",                                    "ADD"),
    Task_Update("Task_Update",                              "UPDATE"),
    Task_Delete("Task_Delete",                              "DELETE"),
    Call_Add("Call_Add",                                    "ADD"),
    Call_Update("Call_Update",                              "UPDATE"),
    Call_Delete("Call_Delete",                              "DELETE"),
    Event_Add("Event_Add",                                  "ADD"),
    Event_Update("Event_Update",                            "UPDATE"),
    Event_Delete("Event_Delete",                            "DELETE"),
    TimeEntry_Add("TimeEntry_Add",                          "ADD"),
    TimeEntry_Update("TimeEntry_Update",                    "UPDATE"),
    TimeEntry_Delete("TimeEntry_Delete",                    "DELETE"),
    Article_Add("Article_Add",                              "ADD"),
    Article_Update("Article_Update",                        "UPDATE"),
    Article_Delete("Article_Delete",                        "DELETE"),
    Article_Translation_Add("Article_Translation_Add",      "ADD"),
    Article_Translation_Update("Article_Translation_Update","UPDATE"),
    Article_Translation_Delete("Article_Translation_Delete","DELETE"),
    Article_Feedback_Add("Article_Feedback_Add",            "ADD"),
    KB_RootCategory_Add("KB_RootCategory_Add",              "ADD"),
    KB_RootCategory_Update("KB_RootCategory_Update",        "UPDATE"),
    KB_RootCategory_Delete("KB_RootCategory_Delete",        "DELETE"),
    KB_Section_Add("KB_Section_Add",                        "ADD"),
    KB_Section_Update("KB_Section_Update",                  "UPDATE"),
    KB_Section_Delete("KB_Section_Delete",                  "DELETE"),

    ;
    String type;
    String tapEvent;
    WebHookEvent(String type,String tapEvent){
        this.type = type;
        this.tapEvent = tapEvent;
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
}
