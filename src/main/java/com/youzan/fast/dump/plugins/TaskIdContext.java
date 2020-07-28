package com.youzan.fast.dump.plugins;

import com.youzan.fast.dump.resolver.DataResolve;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.25
 */
public class TaskIdContext {

    private static Map<String, ResolveSpeed> idResolve = new HashMap<>();

    public static synchronized void put(String id, ResolveSpeed resolve) {
        idResolve.put(id, resolve);
    }

    public static synchronized void remove(String id) {
        idResolve.remove(id);
    }

    public static synchronized ResolveSpeed get(String id) {
        return idResolve.get(id);
    }

    @Data
    @AllArgsConstructor
    public static class ResolveSpeed {
        public int totalNodeSize;
        public DataResolve dataResolve;

        public int changeSpeed(int speed) {
            int nodeSpeed = speed / totalNodeSize;
            if (nodeSpeed > 0) {
                dataResolve.changeSpeed(speed);
            }
            return nodeSpeed;
        }

    }
}
