package com.youzan.fast.dump.plugins.speed;

import org.elasticsearch.action.ActionType;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.24
 */
public class FastReindexSpeedAction extends ActionType<FastReindexSpeedResponse> {

    public static final FastReindexSpeedAction INSTANCE = new FastReindexSpeedAction();
    public static final String NAME = "indices:fast/index/speed";

    public FastReindexSpeedAction() {
        super(NAME, FastReindexSpeedResponse::new);
    }
}
