package com.youzan.fast.dump.common;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.04.15
 */
public enum ResolveTypeEnum {

    ES("ES"),
    HIVE("HIVE"),
    NOT_SUPPORT_RESOLVE("not support resolve");

    private String resolveType;

    ResolveTypeEnum(String resolveType) {
        this.resolveType = resolveType;
    }

    public String getResolveType() {
        return resolveType;
    }

    public static ResolveTypeEnum findResolveTypeEnum(String resolveType) {
        for (ResolveTypeEnum typeEnum : ResolveTypeEnum.values()) {
            if (resolveType.equals(typeEnum.getResolveType())) {
                return typeEnum;
            }
        }
        return NOT_SUPPORT_RESOLVE;
    }
}
