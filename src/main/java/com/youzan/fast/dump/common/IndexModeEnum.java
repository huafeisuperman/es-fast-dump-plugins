package com.youzan.fast.dump.common;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.16
 */
public enum IndexModeEnum {
    CREATE("create"),
    INSERT("insert"),
    UPDATE("update");
    private String mode;

    IndexModeEnum(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static IndexModeEnum findModeEnum(String mode) {
        for (IndexModeEnum modeEnum : IndexModeEnum.values()) {
            if (mode.equals(modeEnum.getMode())) {
                return modeEnum;
            }
        }
        return CREATE;
    }
}
