package com.tapdata.tm.Settings.dto;

import lombok.*;

@AllArgsConstructor
@Getter
@Setter
public class RunNotificationDto extends NotificationDto {
    public enum Label {
        JOB_STARTED("jobStarted"),
        JOB_PAUSED("jobPaused"),
        JOB_DELETED("jobDeleted"),
        JOB_STATE_ERROR("jobStateError"),
        JOB_ENCOUNTER_ERROR("jobEncounterError"),

        CDC_LAG_TIME("CDCLagTime"),
        INSPECT_COUNT("inspectCount"),

        INSPECT_VALUE("inspectValue"),
        INSPECT_DELETE("inspectDelete"),

        INSPECT_ERROR("inspectError") ,


        SERVER_DISCONNECTED("serverDisconnected"),
        AGENT_STARTED("agentStarted"),
        AGENT_STOPPED("agentStopped"),
        AGENT_DELETED("agentDeleted")
        ;

        private String value;

        Label(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}
