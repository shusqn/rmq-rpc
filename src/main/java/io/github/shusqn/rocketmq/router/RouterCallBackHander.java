package io.github.shusqn.rocketmq.router;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * TODO
 * @author Sanjie
 * @date 2020-01-19 17:42
 * @version 1.0
 */
@Slf4j
public abstract class RouterCallBackHander<K, T, R> {

    private final Map<K, Function<T, R>> callHandlers = new ConcurrentHashMap<>();

    /**
     * @param cmd
     * @return 
     * @return
     */
    public R pushCallHandler(K cmd, T obj) {
    	Function<T, R> hander = callHandlers.get(cmd);
    	if(hander != null) {
    		return hander.apply(obj);
    	}
    	return null;
    }
    
    /**
     * @param cmd
     * @param callHandler
     */
    public void registCallHandler(K cmd, Function<T, R> callHandler) {
    	if(callHandlers.get(cmd) != null) {
    		log.error("cmd={} 的callHandlers 已被 注册", cmd);
    		log.error(ExceptionUtils.getStackTrace(new Throwable()));
    		return;
    	}
    	callHandlers.put(cmd, callHandler);
    }

}