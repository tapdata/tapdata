package io.tapdata.zoho.enums;

import io.tapdata.zoho.utils.Checker;

/**
 * the module name with in which search to be done.
 * Value may be :
 * -tickets
 * -contacts
 * -accounts
 * -tasks
 * -calls
 * -events
 * -timeEntry
 * -products
 * -contracts
 * */
public enum FieldModelType {
    TICKETS("tickets"),
    CONTACTS("contacts"),
    ACCOUNTS("accounts"),
    TASKS("tasks"),
    CALLS("calls"),
    EVENTS("events"),
    TIME_ENTRY("timeEntry"),
    PRODUCTS("products"),
    CONTRACTS("contracts"),
    ;
    String model;
    FieldModelType(String model){
        this.model = model;
    }
    public static boolean isModel(String model){
        if (Checker.isEmpty(model)){
            return Boolean.FALSE;
        }
        FieldModelType[] values = values();
        for (FieldModelType value : values) {
            if (value.model.equals(model)) return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }
    public static FieldModelType get(String model){
        if (!FieldModelType.isModel(model)){
            return null;
        }
        FieldModelType[] values = values();
        for (FieldModelType value : values) {
            if (value.model.equals(model)) return value;
        }
        return null;
    }
    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }
}
