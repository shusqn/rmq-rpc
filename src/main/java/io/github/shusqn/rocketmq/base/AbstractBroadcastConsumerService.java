package io.github.shusqn.rocketmq.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Deprecated  //不推荐使用
public abstract class AbstractBroadcastConsumerService{
	private  DefaultMQPushConsumer consumer;
	private  String topic;
    private final String charset = "UTF-8";
    
	/**
	 * @param consumerGroupName
	 * @param topic
	 * @param nameServerAddr
	 * @param instanceName
	 */
	public synchronized void start( String nameServerAddr, String topic, String consumerGroupName, String instanceName) {
		if(consumer != null) {
			log.error("ConsumerService 已经初始化了");
			return;
		}
		this.topic = topic;
		consumer = new DefaultMQPushConsumer(consumerGroupName);
		
		if(instanceName!=null) {
			consumer.setInstanceName(instanceName);
		}
		consumer.setVipChannelEnabled(false);
		consumer.setNamesrvAddr(nameServerAddr);
		consumer.registerMessageListener(new MessageListenerOrderly() {
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				try {
					List<String> bodys = new ArrayList<String>(msgs.size());
					for (MessageExt messageExt : msgs) {
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
		consumer.setConsumeMessageBatchMaxSize(10);
		//设置广播消费
		consumer.setMessageModel(MessageModel.BROADCASTING);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		try {
			consumer.subscribe(this.topic, "*");
			consumer.start();
		} catch (MQClientException e) {
			log.error(e.getMessage(), e);
		}
		
		log.info("AbstractBroadcastConsumerService 初始化成功 {} {} {}", topic, nameServerAddr, consumerGroupName);
	}
	
	protected abstract void onMessage(List<String> messageList);
}
