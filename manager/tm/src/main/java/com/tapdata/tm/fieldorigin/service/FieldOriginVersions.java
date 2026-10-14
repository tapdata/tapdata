package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.fieldorigin.entity.FieldOriginVersionEntity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

final class FieldOriginVersions {
    private FieldOriginVersions() {
    }

    /** Keeps the newest build (its error stays visible), the active version, the newest {@code keep} successful versions and running builds. */
    static List<FieldOriginVersionEntity> prunable(List<FieldOriginVersionEntity> versions, int keep) {
        List<FieldOriginVersionEntity> sorted = new ArrayList<>(versions);
        sorted.sort(Comparator.comparing(FieldOriginVersionEntity::getVersion).reversed());
        List<FieldOriginVersionEntity> result = new ArrayList<>();
        int kept = 0;
        for (int i = 0; i < sorted.size(); i++) {
            FieldOriginVersionEntity version = sorted.get(i);
            if (i == 0 || Boolean.TRUE.equals(version.getActive()) || FieldOriginVersionEntity.STATUS_BUILDING.equals(version.getStatus())) {
                continue;
            }
            if (FieldOriginVersionEntity.STATUS_SUCCESS.equals(version.getStatus()) && kept < keep) {
                kept++;
                continue;
            }
            result.add(version);
        }
        result.sort(Comparator.comparing(FieldOriginVersionEntity::getVersion));
        return result;
    }
}
