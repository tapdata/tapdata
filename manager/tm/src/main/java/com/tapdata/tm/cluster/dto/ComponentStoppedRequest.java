package com.tapdata.tm.cluster.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ComponentStoppedRequest {

    public static final String COMPONENT_ENGINE = "engine";
    public static final String COMPONENT_APISERVER = "apiserver";

    @NotBlank
    private String uuid;

    @NotBlank
    private String component;

    private String processId;
}
