package com.youzan.fast.dump.common;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.utils
 * @Description :  TODO
 * @Creation Date:  2018-08-16 15:58
 * --------  ---------  --------------------------
 */
public enum StatusEnum {
    SUCCESS("0"),
    PROCESSING("1"),
    FAILED("2"),
    //文件状态发送后，就变为FINAL状态
    FINAL("3");

    private String status;

    StatusEnum(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static StatusEnum findStatusEnum(String status) {
        for (StatusEnum statusEnum : StatusEnum.values()){
            if (status.equals(statusEnum.getStatus())){
                return statusEnum;
            }
        }
        return PROCESSING;
    }


}
