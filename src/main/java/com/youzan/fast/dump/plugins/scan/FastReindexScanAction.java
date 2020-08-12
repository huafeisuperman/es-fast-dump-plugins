package com.youzan.fast.dump.plugins.scan;

import org.elasticsearch.action.ActionType;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.08.07
 */
public class FastReindexScanAction extends ActionType<FastReindexScanResponse> {

    public static final FastReindexScanAction INSTANCE = new FastReindexScanAction();
    public static final String NAME = "indices:data/write/fast/index/scan";

    public FastReindexScanAction() {
        super(NAME, FastReindexScanResponse::new);
    }
}
