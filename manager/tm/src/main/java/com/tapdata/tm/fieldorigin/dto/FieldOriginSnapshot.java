package com.tapdata.tm.fieldorigin.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class FieldOriginSnapshot {
    private FieldOriginVersionDto version;
    private List<FieldOriginDto> origins = new ArrayList<>();
}
