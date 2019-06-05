package com.ef.enums;

public enum DurationEnum {
    HOURLY("hourly"),
    DAILY("daily");

    String val;

    DurationEnum(String duration) {
        val = duration;
    }

    public String getVal() {
        return DurationEnum.valueOf(val.toUpperCase()).name();
    }
}
