package com.frame.rocketmq.base;

import java.util.List;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import com.alibaba.fastjson.JSON;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractProducerService {
	private DefaultMQProducer producer;
	private SendCallback sendCallback;
	private MessageQueueSelector messageQueueSelector;
	private final String charset = "UTF-8";

	/**
	 * @param nameServerAddr
	 * @param producerGroupName
	 * @param queueNum
	 * @param instanceName
	 * @param accessKey
	 * @param secretKey
	 */
	public synchronized void start(String nameServerAddr, String producerGroupName,  int queueNum, String instanceName, String accessKey, String  secretKey) {
		producer = new DefaultMQProducer(producerGroupName, new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)));
		startProducer(nameServerAddr, producerGroupName, queueNum, instanceName);
	}
	/**
	 * 启动mq生产器
	 * @param nameServerAddr
	 * @param producerGroupName
	 * @param queueNum
	 * @param instanceName
	 */
	public synchronized void start(String nameServerAddr, String producerGroupName,  int queueNum, String instanceName) {
		if(producer != null) {
			log.error("ProducerService 已被初始化");
			throw new RuntimeException("ProducerService 已被初始化");
		}
		producer = new DefaultMQProducer(producerGroupName);
		startProducer(nameServerAddr, producerGroupName, queueNum, instanceName);
	}

	private void startProducer(String nameServerAddr, String producerGroupName,  int queueNum, String instanceName) {
		producer.setNamesrvAddr(nameServerAddr);
		//重试次数
		producer.setRetryTimesWhenSendFailed(2);
		if(instanceName !=null) {
			producer.setInstanceName(instanceName);
		}
		producer.setDefaultTopicQueueNums(queueNum);
		producer.setSendMessageWithVIPChannel(false);
		producer.setRetryTimesWhenSendAsyncFailed(2);
		producer.setRetryTimesWhenSendFailed(2);
		
		producer.setSendMsgTimeout(5 * 1000);
		sendCallback = new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
			}
			@Override
			public void onException(Throwable e) {
				log.error(e.getMessage(), e);
			}
		};
		
		messageQueueSelector = new MessageQueueSelector() {
			@Override
			public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
				long id = (long) arg;
				int index = (int) (id % mqs.size());
				if(index < 0) {
					index = Math.abs(index);
				}
				return mqs.get(index);
			}
		};
		
		try {
			producer.start();
		} catch (MQClientException e) {
			log.error(e.getMessage(), e);
		}
		log.info("ProducerService 初始化成功 {} {}", nameServerAddr, producerGroupName);
	}

	/**
	 * 发送消息
	 * @param topic
	 * @param data
	 * @param roleId
	 * @return
	 */
	public boolean sendMessage(String topic, Object data, long roleId){
		return sendMessage(topic, data, roleId, true, true);
	}
	
	public boolean showSendLogs = true;
	/**
	 * @param topic
	 * @param data
	 * @param roleId
	 * @param callBack 是否有返回回调
	 * @param async 是否异步发送
	 * @return
	 */
	public boolean sendMessage(String topic, Object data, long roleId, boolean callBack, boolean async){
		if(producer == null) {
			log.error("ProducerService 未初始化");
			return false;
		}
		String jsonData = JSON.toJSONString(data);
		try {
			if(showSendLogs) {
				log.info("send topic:{} data:{}", topic, jsonData);
			}
			byte[] body = jsonData.getBytes(charset);
			Message message = new Message(topic, body);
			if(!callBack) {
				producer.sendOneway(message, messageQueueSelector, roleId);
				return true;
			}
			if(async) {
				//异步
				producer.send(message, messageQueueSelector, roleId, sendCallback);
				return true;
			}
			//同步
			SendResult  sendResult  = producer.send(message, messageQueueSelector, roleId);
			if(SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
				return true;
			}
			else {
				log.error("m send error>>> {} {}", roleId, jsonData);
				return false;
			}
		}
		catch (Exception e){
			log.error("topic:{} sendFail", topic);
			log.error(e.getMessage(), e);
		}
		return false;
	}
}
