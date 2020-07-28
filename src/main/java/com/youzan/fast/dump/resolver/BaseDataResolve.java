package com.youzan.fast.dump.resolver;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    protected Logger logger = LogManager.getLogger(getClass());

    public BaseDataResolve(int speedLimit) {
        rateLimiter = RateLimiter.create(speedLimit);
    }

    @Override
    public void afterFinish() throws Exception {
    }

    @Override
    public void commit() throws Exception {

    }

    @Override
    public void changeSpeed(int speed) {
        int oldSpeed = (int) (rateLimiter.getRate());
        rateLimiter = RateLimiter.create(speed);
        logger.info("speed change[{}->{}]", oldSpeed, speed);
    }


}
