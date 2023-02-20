package com.tapdata.tm.commons.schema;

import lombok.Getter;

public enum ScheduleTimeEnum {
    FALSE("false", 100),
    ZERO("00:00", 0),
    ONE("01:00", 1),
    TWO("02:00", 2),
    THREE("03:00", 3),
    FOUR("04:00", 4),
    FIVE("05:00", 5),
    SIX("06:00", 6),
    SEVEN("07:00", 7),
    EIGHT("08:00", 8),
    NINE("09:00", 9),
    TEN("10:00", 10),
    ELEVEN("11:00", 11),
    TWELVE("12:00", 12),
    THIRTEEN("13:00", 13),
    FOURTEEN("14:00", 14),
    FIFTEEN("15:00", 15),
    SIXTEEN("16:00", 16),
    SEVENTEEN("17:00", 17),
    EIGHTEEN("18:00", 18),
    NINETEEN("19:00", 19),
    TWENTY("20:00", 20),
    TWENTY_ONE("21:00", 21),
    TWENTY_TWO("22:00", 22),
    TWENTY_THREE("23:00", 23),
    ;



    @Getter
    private final String key;
    @Getter
    private final int value;

    ScheduleTimeEnum(String key, int value) {
        this.key = key;
        this.value = value;
    }

    public static int getHour(String key) {
        for (ScheduleTimeEnum value : ScheduleTimeEnum.values()) {
            if (value.key.equals(key)) {
                return value.getValue();
            }
        }
        return ScheduleTimeEnum.TWO.getValue();
    }
}