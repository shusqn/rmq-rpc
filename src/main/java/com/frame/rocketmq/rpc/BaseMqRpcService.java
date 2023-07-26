package com.frame.rocketmq.rpc;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.frame.rocketmq.base.AbstractConsumerService;
import com.frame.rocketmq.base.AbstractProducerService;
import com.frame.rocketmq.router.RouterCallBackHander;
import com.frame.rocketmq.router.RouterHander;
import com.frame.rocketmq.rpc.aopProxy.RpcMqClient;
import com.frame.rocketmq.rpc.aopProxy.RpcMqServer;
import com.frame.rocketmq.utils.ClassUtil;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public abstract class BaseMqRpcService implements BeanDefinitionRegistryPostProcessor, ApplicationContextAware{
	//=============================static===============================
	/**
	 * 是否异步
	 */
	protected boolean asyn = false;
	
	public final int TIME_OUT = 120;
	/**
	 * req_rpc_server_
	 */
	private static final String REQ_RPC_SERVER = "req_rpc_server_";
	/**
	 * resp_rpc_server_
	 */
	private static final String RESP_RPC_SERVER = "resp_rpc_server_";
	/**
	 * consumer_resp_rpc_server_
	 */
	private static final String CONSUMER_RESP_RPC_SERVER = "consumer_resp_rpc_server_";
	/**
	 *  consumer_req_rpc_server_
	 */
	private static final String CONSUMER_REQ_RPC_SERVER = "consumer_req_rpc_server_";
	/**
	 * producer_rpc_group
	 */
	private static final String PRODUCER_RPC_GROUP = "producer_rpc_group";
	/**
	 * id生成器
	 */
	private static AtomicLong msgIdBuilder = new AtomicLong(0);
	/**
	 * 单例线程池
	 */
	private static ScheduledThreadPoolExecutor mqRpcExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r, "MqRpcExecutor");
			thread.setDaemon(false);
			return thread;
		}
	});
	//================================================================
	/**
	 * rpc 请求接收器
	 */
	private MqRpcReceive mqReqReceive;
	/**
	 * rpc 返回接收器
	 */
	private MqRpcReceive mqRespReceive;
	/**
	 * rpc 数据发送器
	 */
	private  AbstractProducerService rocketMqSender = new AbstractProducerService() {};
	/**
	 * rpc 请求 回调函数 路由器
	 */
	private RouterCallBackHander<String, Object[], Object> rpcRespRouter = new RouterCallBackHander<String, Object[], Object>() {};
	/**
	 * rpc type 类型路由器
	 */
	private RouterHander<MqRpcData> rpcReqRouter = new RouterHander<MqRpcData>() {};
	private int localServerId;
	private boolean initOK = false;
	
	/**
	 * @param nameServerAddr
	 * @param localServerId
	 */
	protected synchronized void initClient(String nameServerAddr, int localServerId) {
		 init(nameServerAddr, localServerId, false, null);
	}
	
	/**
	 * @param nameServerAddr
	 * @param localServerId
	 */
	protected synchronized void initServer(String nameServerAddr, int localServerId, @NonNull  String targetServer) {
		 init(nameServerAddr, localServerId, false, targetServer);
	}
	
	/**
	 * @param nameServerAddr
	 * @param localServerId
	 */
	protected synchronized void initClientAndServer(String nameServerAddr, int localServerId, @NonNull  String targetServer) {
		 init(nameServerAddr, localServerId, true, targetServer);
	}
	
	/**
	 * 初始化mq 地址和本地serverId
	 * @param nameServerAddr
	 * @param localServerId
	 * @param req
	 * @param resp
	 * @param serverType
	 * @param accessKeyAndsecretKey
	 */
	protected synchronized void init(String nameServerAddr, int localServerId, Boolean clientAndSerever, String serverType, String... accessKeyAndsecretKey) {
		log.info("init RocketMqRpcService");
		if(initOK) {
			throw new RuntimeException("非法操作, RocketMqRpcService 已被初始化");
		}
		if(accessKeyAndsecretKey.length == 2) {
			//初始化mq
			rocketMqSender.start(nameServerAddr, PRODUCER_RPC_GROUP+localServerId, 9, null,  accessKeyAndsecretKey[0],  accessKeyAndsecretKey[1]);
			
			if(clientAndSerever || serverType == null) {
				mqReqReceive = new MqRpcReceive(MqRpcReceive.TYPE_REQ);
				mqReqReceive.start(nameServerAddr, 
						REQ_RPC_SERVER + localServerId, 
						CONSUMER_REQ_RPC_SERVER + localServerId,
						CONSUMER_REQ_RPC_SERVER + localServerId, accessKeyAndsecretKey[0],  accessKeyAndsecretKey[1], true);
			}
			if(serverType != null) {
				mqRespReceive = new MqRpcReceive(MqRpcReceive.TYPE_RESP);
				mqRespReceive.start(nameServerAddr, 
						RESP_RPC_SERVER + serverType, 
						CONSUMER_RESP_RPC_SERVER + serverType,
						CONSUMER_RESP_RPC_SERVER + serverType, accessKeyAndsecretKey[0],  accessKeyAndsecretKey[1], true);
			}
		}
		else {
			//初始化mq
			rocketMqSender.start(nameServerAddr, PRODUCER_RPC_GROUP+localServerId, 9, null);
			
			if(clientAndSerever || serverType == null) {
				mqReqReceive = new MqRpcReceive(MqRpcReceive.TYPE_REQ);
				mqReqReceive.start(nameServerAddr, 
						REQ_RPC_SERVER + localServerId, 
						CONSUMER_REQ_RPC_SERVER + localServerId,
						CONSUMER_REQ_RPC_SERVER + localServerId, true);
			}
			if(serverType != null) {
				mqRespReceive = new MqRpcReceive(MqRpcReceive.TYPE_RESP);
				mqRespReceive.start(nameServerAddr, 
						RESP_RPC_SERVER + serverType, 
						CONSUMER_RESP_RPC_SERVER + serverType,
						CONSUMER_RESP_RPC_SERVER + serverType, true);
			}
		}
		rocketMqSender.showSendLogs = false;
		this.localServerId = localServerId;
		initOK = true;
	}
	
	/**
	 * @param mqRpcType
	 * @param backFunc
	 */
	public void registBackFuncByType(String reqMapping, Function<Object[], Object> backFunc) {
		rpcRespRouter.registCallHandler(reqMapping, backFunc);
		mqRpcExecutor.schedule(()->{
			if(!initOK) {
				try {
					throw new RuntimeException("非法操作, RocketMqRpcService 未被初始化:"+reqMapping);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			}
		}, 30, TimeUnit.SECONDS);
	}
	
	/**
	 * 订阅发布模式，发布消息
	 * @param backFunc
	 * @param msgId
	 * @param type
	 * @param toServerId
	 * @param rs
	 */
	protected MqRpcData sendAndReceiveRpcMsg(Consumer<MqRpcData> backFunc, String reqMapping, Object serverName, Object[] args) {
		if(!initOK) {
			throw new RuntimeException("非法操作, RocketMqRpcService 未被初始化");
		}
		long msgId = msgIdBuilder.getAndIncrement();
		MqRpcData data = MqRpcData.build(msgId, reqMapping, localServerId, serverName, args);
		rocketMqSender.sendMessage(RESP_RPC_SERVER +data.getServer(), data, data.getMsgId());
		rpcReqRouter.registHandler(msgId, backFunc);
		log.debug("sendRpc msgId:{} data:{}", data.getMsgId(), JSON.toJSON(data));
		
		mqRpcExecutor.schedule(()->{
			rpcReqRouter.removeCallHandler(msgId);
		}, TIME_OUT, TimeUnit.SECONDS);
		
		return data;
	}

	private final class MqRpcReceive extends AbstractConsumerService {
		/**
		 * 请求端
		 */
		private static final int TYPE_REQ = 1;
		/**
		 * 响应端
		 */
		private static final int TYPE_RESP = 2;
		private final int type;
		public MqRpcReceive(int type) {
			this.type = type; 
		}
		
		@Override
		protected void onMessage(List<String> messageList) {
			for (String message : messageList) {
				receiveMqData(message);
			}
		}

		/**
		 * @param message
		 */
		private void receiveMqData(String message) {
			MqRpcData data = JSON.parseObject(message, MqRpcData.class, Feature.SupportNonPublicField);
			log.debug("getRpc msgId:{} type:{} data:{}",data.getMsgId(), type == 1 ? "reqtype":"resptype", message);
			if(type == TYPE_RESP) {
				try {
					Object rs = rpcRespRouter.pushCallHandler(data.getReqMapping(), data.getArgs());
					data.setResult( rs );
				} catch (Exception e) {
					data.setCode(500, ExceptionUtils.getStackTrace(e));
					log.error(e.getMessage(), e);
				}
				rocketMqSender.sendMessage(REQ_RPC_SERVER +data.getClient(), data, data.getMsgId());
			}
			else if(type == TYPE_REQ) {
				rpcReqRouter.pushHandler(data.getMsgId(), data);
			}
		}
	}
	
	/**
	 * 
	 */
	public void shutdown() {
		mqRespReceive.shutdown();
	}
	
	//====================================================================
	private ApplicationContext ctx;
	private static String mainPackage;
	
	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		if(asyn) {
			return;
		}
		List<Class<?>> list = ClassUtil.getClasses(mainPackage);
		for (Class<?> cls : list) {
			if (cls.getAnnotation(RpcMqClient.class) != null) {
				BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(cls);
				GenericBeanDefinition definition = (GenericBeanDefinition) builder.getRawBeanDefinition();
				definition.getPropertyValues().add("interfaceClass", definition.getBeanClassName());
				definition.setBeanClass(RpcMqClentAopProxyFactory.class);
				definition.setAutowireMode(GenericBeanDefinition.AUTOWIRE_BY_TYPE);
				String beanName = cls.getSimpleName().substring(1, 2).toLowerCase()
						+ cls.getSimpleName().substring(2, cls.getSimpleName().length());
				registry.registerBeanDefinition(beanName, definition);

				log.info("{} {}", cls.getName(), beanName);
			}
		}
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ctx = applicationContext;
		
		mainPackage = ctx.getBeansWithAnnotation(SpringBootApplication.class).values().stream().findFirst().get()
				.getClass().getPackage().getName();
		
		registReqRspMapping(mainPackage);
	}
	
	
	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

	}
	
	/**
	 * @param packageName
	 */
	private void registReqRspMapping(String packageName) {
		List<Class<?>> list = ClassUtil.getClasses(packageName);
		List<Class<?>> superClsList = new ArrayList<Class<?>>();
		for (Class<?> cls : list) {
			if (cls.getAnnotation(RpcMqServer.class) != null) {
				superClsList.add(cls);
			}
		}
		for (Class<?> superCls : superClsList) {
			for (Class<?> cls : list) {
				if (cls.getInterfaces().length > 0) {
					Class<?> proxyInterface = null;
					for (Class<?> proxyInterfaceTemp : cls.getInterfaces()) {
						if (proxyInterfaceTemp.getName().equals(superCls.getName())) {
							proxyInterface = proxyInterfaceTemp;
							break;
						}
					}
					if (proxyInterface == null) {
						continue;
					}
					// ==============================
					for (Method method : proxyInterface.getMethods()) {
						String reqMapping = proxyInterface.getSimpleName() + "/" + method.getName();
						log.debug("mqRpcServer regist {} {}", cls.getName(), reqMapping);
						// private 也可以访问
						method.setAccessible(true);
						try {
							final Object model = cls.newInstance();
							registBackFuncByType(reqMapping, (k) -> {
								try {
									Object[] arrObjs = k;
									return method.invoke(model, arrObjs);
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							});
						} catch (InstantiationException | IllegalAccessException e1) {
							log.error(e1.getMessage(), e1);
						}
					}
					// ===========================
				}
			}
		}
	}
	

}
