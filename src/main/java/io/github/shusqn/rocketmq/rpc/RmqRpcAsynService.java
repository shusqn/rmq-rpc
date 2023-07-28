package io.github.shusqn.rocketmq.rpc;

import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * 异步请求
 * RmqRpcAsynService.java
 * @date 2023-07-26 15:41
 * @version 1.0.0
 */
@Slf4j
@Service
public final class RmqRpcAsynService {
	
	private static RmqRpcAsynService instance;
	
	@PostConstruct
	private void init() {
		instance = this;
		//instance.asyn =  true;
	}
	/**
	 * @param nameServerAddr
	 * @param localServerId
	 */
	public static void registerClient(String nameServerAddr, int localServerId) {
		log.info("registerClient");

		//instance.initClient(nameServerAddr, localServerId);
	}

	/**
	 * 远程调用
	 * @param <T>
	 * @param backFunc
	 * @param reqMapping
	 * @param serverName
	 * @param args
	 */
	public static void remoteCallFunc(Consumer<Rmq_data> backFunc, String reqMapping, Object serverName, Object[] args) {
		//instance.sendAndReceiveRpcMsg(backFunc, reqMapping, serverName, args);
	}
		
}