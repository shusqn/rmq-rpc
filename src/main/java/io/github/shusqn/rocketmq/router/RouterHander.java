package io.github.shusqn.rocketmq.router;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

/**
 * TODO
 * @author Sanjie
 * @date 2020-01-19 17:42
 * @version 1.0
 */
@Slf4j
public abstract class RouterHander<T> {

    private final Map<Long, Consumer<T>> handlers = new ConcurrentHashMap<>();

    /**
     * 获取Handler
     * @param cmd
     * @return
     */
    public Consumer<T> getHandler(long cmd) {
    	Consumer<T> hander = handlers.get(cmd);
    	return hander;
    }
    
    /**
     * 请求回调函数
     * @param cmd
     * @param obj
     */
    public void pushHandler(long cmd, T obj) {
    	Consumer<T> hander = handlers.get(cmd);
    	if(hander != null) {
    		hander.accept(obj);
    	}
    	else {
    		log.error("cmd = {} handler  not regist------" + cmd);
    	}
    }
    
    /**
     * 注册逻辑handler
     *
     * @param cmd
     * @param logicHandler
     */
    public void registHandler(long cmd, Consumer<T> logicHandler) {
    	if(handlers.get(cmd) != null) {
    		log.error("cmd={} 的handler 已被 注册", cmd);
    		return;
    	}
        handlers.put(cmd, logicHandler);
    }
    
    /**
     * @param cmd
     */
    public void removeCallHandler(long cmd) {
    	handlers.remove(cmd);
    }

}