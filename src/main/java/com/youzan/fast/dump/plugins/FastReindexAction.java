package com.youzan.fast.dump.plugins;

import org.elasticsearch.action.ActionType;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.03.25
 */
public class FastReindexAction extends ActionType<FastReindexResponse> {
    public static final FastReindexAction INSTANCE = new FastReindexAction();
    public static final String NAME = "indices:fast/index";

    public FastReindexAction() {
        super(NAME, FastReindexResponse::new);
    }
}
