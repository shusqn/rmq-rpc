package io.github.shusqn.rocketmq.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * AbstractConsumerService.java
 * @author Sanjie
 * @date 2021-09-10 09:58
 * @version 1.0.0
 */
@Slf4j
public abstract class AbstractConsumerService{
	@Getter
	private  DefaultMQPushConsumer consumer;
	@Getter
	private  String topic;
    private final String charset = "UTF-8";
    private long startTimestamp;
    
    /**
     * @param nameServerAddr
     * @param topic
     * @param consumerGroupName
     * @param instanceName
     * @param accessKey
     * @param secretKey
     */
    public void start( String nameServerAddr, String topic, String consumerGroupName, String instanceName, String accessKey, String  secretKey, boolean... cleanAllBeforeMsg) {
		if(consumer != null) {
			log.error("ConsumerService 已经初始化了 nameServerAddr:{} topic:{}", nameServerAddr, topic);
			throw new RuntimeException("ConsumerService 已经初始化了");
		}
    	consumer = new DefaultMQPushConsumer(consumerGroupName, new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)), new AllocateMessageQueueAveragely());
    	if(cleanAllBeforeMsg.length == 0) {
			startConsumer(nameServerAddr, topic, consumerGroupName, instanceName, false);
		}
		else {
			startConsumer(nameServerAddr, topic, consumerGroupName, instanceName, true);
		}
    }
    
	/**
	 * @param consumerGroupName
	 * @param topic
	 * @param nameServerAddr
	 * @param instanceName
	 */
    public void start( String nameServerAddr, String topic, String consumerGroupName, String instanceName, boolean... cleanAllBeforeMsg) {
		if(consumer != null) {
			log.error("ConsumerService 已经初始化了 nameServerAddr:{} topic:{}", nameServerAddr, topic);
			throw new RuntimeException("ConsumerService 已经初始化了");
		}
		consumer = new DefaultMQPushConsumer(consumerGroupName);
		if(cleanAllBeforeMsg.length == 0) {
			startConsumer(nameServerAddr, topic, consumerGroupName, instanceName, false);
		}
		else {
			startConsumer(nameServerAddr, topic, consumerGroupName, instanceName, true);
		}
	}
    
    /**
     * @param nameServerAddr
     * @param topic
     * @param consumerGroupName
     * @param instanceName
     * @param cleanAllBeforeMsg
     */
    private void startConsumer( String nameServerAddr, String topic, String consumerGroupName, String instanceName, final boolean cleanAllBeforeMsg) {
    	this.topic = topic;
		if(instanceName !=null) {
			consumer.setInstanceName(instanceName);
		}
		consumer.setNamesrvAddr(nameServerAddr);
		consumer.setVipChannelEnabled(false);
		consumer.registerMessageListener(new MessageListenerOrderly() {
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				try {
					List<String> bodys = new ArrayList<String>(msgs.size());
					for (MessageExt messageExt : msgs) {
						//清除启动3秒前的所有msg
						if(cleanAllBeforeMsg && messageExt.getBornTimestamp() + 10 * 1000 < startTimestamp) {
							log.info("删除过时的消息 msgid:{} data:{}", messageExt.getMsgId(), new String(messageExt.getBody(), charset));
							continue;
						}
						byte[] body = messageExt.getBody();
						bodys.add(new String(body, charset));
					}
					onMessage(bodys);
				}
				catch (Exception e){
					log.error(e.getMessage(), e);
				}
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});
		// 批量消费,每次拉取10条
		consumer.setConsumeMessageBatchMaxSize(50);
		//设置广播消费
		consumer.setMessageModel(MessageModel.CLUSTERING);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		try {
			startTimestamp = System.currentTimeMillis();
			consumer.subscribe(this.topic, "*");
			consumer.start();
		} catch (MQClientException e) {
			log.error(e.getMessage(), e);
		}
		log.info("ConsumerService 初始化成功 nameServerAddr:{} topic:{}", nameServerAddr, topic);
    }
	
	/**
	 * 接受消息
	 * @param messageList
	 */
	protected abstract void onMessage(List<String> messageList);
	
	/**
	 * 关闭
	 */
	public void shutdown() {
		consumer.shutdown();
	}
}
