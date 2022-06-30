package com.tapdata.tm.events.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FailResult {
    private Long retry;
    private Long next_retry;
    private String fail_message;
    private Long ts;
}
