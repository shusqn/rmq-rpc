package io.github.shusqn.rocketmq.rpc;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/*
同步请求
 * RmqRpcService.java
 * @date 2023-07-26 15:41
 * @version 1.0.0
 */
@Slf4j
@Service
public final class RmqRpcService  extends BaseMqRpcService{
	
	private static Map<Long, ArrayBlockingQueue<MqRpcData>> backFuncMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<MqRpcData>>();
	private static ArrayBlockingQueue<ArrayBlockingQueue<MqRpcData>> waitQueue = new ArrayBlockingQueue<>(10240);

	private static RmqRpcService instance;
	
	@PostConstruct
	private void init() {
		instance = this;
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
	 * @param data
	 */
	private static void backFunc(MqRpcData data) {
		ArrayBlockingQueue<MqRpcData> handerQueue = backFuncMap.remove(data.getMsgId());
		if (handerQueue != null) {
			handerQueue.offer(data);
		}
	}

	/**
	 * @param <T>
	 * @param <T>
	 * @param reqMapping
	 * @param toServerId
	 * @param reqJson
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected static <T> T callBack(String reqMapping, Object server, Object... args) {
		ArrayBlockingQueue<MqRpcData> handerQueue = waitQueue.poll();
		if (handerQueue == null) {
			handerQueue = new ArrayBlockingQueue<>(1);
		}
		MqRpcData sendData = instance.sendAndReceiveRpcMsg(RmqRpcService::backFunc, reqMapping, server, args);
		backFuncMap.put(sendData.getMsgId(), handerQueue);

		MqRpcData backData = null;
		try {
			backData = handerQueue.poll(instance.TIME_OUT, TimeUnit.SECONDS);
			if (backData == null) {
				backFuncMap.remove(sendData.getMsgId());
				throw new RuntimeException("callBack time out 120 seconds");
			} else if (handerQueue.size() == 0 && backData != null) {
				waitQueue.offer(handerQueue);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (backData.getCode() != 0) {
			throw new RuntimeException(backData.getErr());
		}
		try {
			return (T) JSON.parseObject(backData.getRs(), Class.forName(backData.getRsClass()),
					Feature.SupportNonPublicField);
		} catch (Exception e) {
			log.error(e.getMessage());
			return (T) JSON.parseObject(backData.getRs(), Object.class, Feature.SupportNonPublicField);
		}
	}
}