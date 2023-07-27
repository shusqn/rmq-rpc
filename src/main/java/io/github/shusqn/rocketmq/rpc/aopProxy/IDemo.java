package io.github.shusqn.rocketmq.rpc.aopProxy;

import java.util.Map;

import org.springframework.stereotype.Service;

@Service
@RpcMqClient(targetServer = "rmq-server-type")
@RpcMqServer
public interface IDemo {
	Map<String, Long> updataUserBalance(long userId, long balance);
	String hello(String name);
}
