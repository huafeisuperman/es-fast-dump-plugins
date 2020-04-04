package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.RateLimiter;

import java.util.List;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.resolver
 * @Description :  TODO
 * @Creation Date:  2018-08-16 9:35
 * --------  ---------  --------------------------
 */
public abstract class BaseDataResolve implements DataResolve<List<JSONObject>> {


    protected RateLimiter rateLimiter;

    public BaseDataResolve(int speedLimit) {
        rateLimiter = RateLimiter.create(speedLimit);
    }

    @Override
    public void afterFinish() throws Exception {
    }

    @Override
    public void commit() throws Exception {

    }
}
