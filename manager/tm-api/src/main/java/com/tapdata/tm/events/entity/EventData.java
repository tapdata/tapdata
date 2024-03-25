package com.tapdata.tm.events.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventData {
    private String title;
    private String message;
}
