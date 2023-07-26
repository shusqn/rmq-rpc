package com.frame.rocketmq.rpc;

import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * 异步请求
 * RmqRpcAsynService.java
 * @date 2023-07-26 15:41
 * @version 1.0.0
 */
@Slf4j
@Service
public final class RmqRpcAsynService  extends BaseMqRpcService{
	
	private static RmqRpcAsynService instance;
	
	@PostConstruct
	private void init() {
		instance = this;
		instance.asyn =  true;
	}
	/**
	 * @param nameServerAddr
	 * @param localServerId
	 */
	public static void registerClient(String nameServerAddr, int localServerId) {
		log.info("registerClient");

		instance.initClient(nameServerAddr, localServerId);
	}

	/**
	 * @param nameServerAddr
	 * @param localServerId
	 * @param targetServer
	 */
	public static void registerServer(String nameServerAddr, int localServerId, @NonNull String targetServer) {
		log.info("registerClient");

		instance.initServer(nameServerAddr, localServerId, targetServer);
	}
	
	/**
	 * 远程调用
	 * @param <T>
	 * @param backFunc
	 * @param reqMapping
	 * @param serverName
	 * @param args
	 */
	public static void remoteCallFunc(Consumer<MqRpcData> backFunc, String reqMapping, Object serverName, Object[] args) {
		instance.sendAndReceiveRpcMsg(backFunc, reqMapping, serverName, args);
	}
		
}