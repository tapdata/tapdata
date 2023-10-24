package io.tapdata.pdk.apis.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectorCapabilities {
    public static ConnectorCapabilities create() {
        return new ConnectorCapabilities();
    }
    private List<String> disableCapabilities;
    public ConnectorCapabilities disable(String capabilityId) {
        if(disableCapabilities == null)
            disableCapabilities = new ArrayList<>();
        disableCapabilities.add(capabilityId);
        return this;
    }
    private Map<String, String> capabilityAlternativeMap;
    public ConnectorCapabilities alternative(String capabilityId, String alternative) {
        if(capabilityAlternativeMap == null)
            capabilityAlternativeMap = new HashMap<>();
        capabilityAlternativeMap.put(capabilityId, alternative);
        return this;
    }

    public boolean isDisabled(String capabilityId) {
        return disableCapabilities != null && disableCapabilities.contains(capabilityId);
    }

    public String getCapabilityAlternative(String capabilityId) {
        if(capabilityAlternativeMap != null)
            return capabilityAlternativeMap.get(capabilityId);
        return null;
    }

    public Map<String, String> getCapabilityAlternativeMap() {
        return capabilityAlternativeMap;
    }

    public void setCapabilityAlternativeMap(Map<String, String> capabilityAlternativeMap) {
        this.capabilityAlternativeMap = capabilityAlternativeMap;
    }

    public List<String> getDisableCapabilities() {
        return disableCapabilities;
    }

    public void setDisableCapabilities(List<String> disableCapabilities) {
        this.disableCapabilities = disableCapabilities;
    }
}
