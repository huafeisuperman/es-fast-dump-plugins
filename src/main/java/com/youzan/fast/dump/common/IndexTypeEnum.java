package com.youzan.fast.dump.common;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2019.07.15
 */
public enum IndexTypeEnum {

    ALL_TO_ONE("all_to_one"),

    ALL_TO_ALL("all_to_all"),

    ONE_TO_ONE("one_to_one"),

    CUSTOM("custom"),

    NOT_SUPPORT_TYPE("not support_type");

    private String indexType;

    IndexTypeEnum(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexType() {
        return indexType;
    }

    public static IndexTypeEnum findIndexTypeEnum(String indexType) {
        for (IndexTypeEnum indexTypeEnum : IndexTypeEnum.values()){
            if (indexType.equals(indexTypeEnum.getIndexType())){
                return indexTypeEnum;
            }
        }
        return NOT_SUPPORT_TYPE;
    }
}
