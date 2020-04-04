package com.youzan.fast.dump.client;

/**
 * @author :  43333
 * @Project Name :  mainline
 * @Package Name :  com.youzan.clouddb.tools.general.client
 * @Description :  TODO
 * @Creation Date:  2018-08-28 10:28
 * --------  ---------  --------------------------
 */
public interface Client<T> {

    T getClient() throws Exception;
}
