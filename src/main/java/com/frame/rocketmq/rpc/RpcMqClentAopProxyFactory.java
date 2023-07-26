package com.frame.rocketmq.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.beans.factory.FactoryBean;

import com.frame.rocketmq.rpc.aopProxy.RpcMqClient;

/**
 * RmqRpcService.java
 * @date 2023-07-26 14:48
 * @version 1.0.0
 */
public class RpcMqClentAopProxyFactory<T> implements FactoryBean<T>, InvocationHandler {

	private Class<T> interfaceClass;

	public Class<T> getInterfaceClass() {
		return interfaceClass;
	}

	public void setInterfaceClass(Class<T> interfaceClass) {
		this.interfaceClass = interfaceClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T getObject() throws Exception {
		return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{interfaceClass}, this);
	}

	@Override
	public Class<?> getObjectType() {
		return interfaceClass;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 真正执行的方法
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?>  proxyInterface =proxy.getClass().getInterfaces()[0];
		
		String targetServer = proxyInterface.getDeclaredAnnotation(RpcMqClient.class).targetServer();
		String reqMapping = proxyInterface.getSimpleName() +"/" +method.getName();
		
		return RmqRpcService.callBack(reqMapping, targetServer, args);
	}
}