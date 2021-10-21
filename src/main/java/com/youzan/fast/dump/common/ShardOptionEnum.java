package com.youzan.fast.dump.common;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.05
 */
public enum ShardOptionEnum {

    PRIMARY("p"),
    REPLICA("r"),
    DEFAULT("default");

    private String shardOption;

    ShardOptionEnum(String shardOptions) {
        this.shardOption = shardOptions;
    }

    public String getShardOption() {
        return shardOption;
    }

    public static ShardOptionEnum findShardOption(String shardOption) {
        for (ShardOptionEnum shardEnum : ShardOptionEnum.values()) {
            if (shardOption.equals(shardEnum.getShardOption())) {
                return shardEnum;
            }
        }
        return DEFAULT;
    }
}
