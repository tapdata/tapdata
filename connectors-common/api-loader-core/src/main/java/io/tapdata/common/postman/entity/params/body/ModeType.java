package io.tapdata.common.postman.entity.params.body;

import io.tapdata.common.postman.entity.params.Body;

import java.util.Objects;

public enum ModeType {
        NO_ONE("noone", NoOne.class),
        FORM_DATA("formdata", FormData.class),
        FORM_URL_ENCODED("urlencoded", FormUrlEncoded.class),
        ROW("raw", Row.class),
        BINARY("file", Binary.class),
        GRAPHQL("graphql", GraphQl.class);
        String modeName;
        Class<? extends Body> cla;

        ModeType(String mode, Class<? extends Body> cla) {
            this.cla = cla;
            this.modeName = mode;
        }
        public static Class<? extends Body> getByMode(String mode){
            if (Objects.nonNull(mode) && !mode.trim().equals("")) {
                ModeType[] values = values();
                for (ModeType value : values) {
                    if (value.modeName.equals(mode.trim())) return value.cla;
                }
            }
            return NO_ONE.cla;
        }
    }