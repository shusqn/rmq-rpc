package com.frame.rocketmq.rpc;

import com.alibaba.fastjson.JSON;

public class MqRpcData{
	private long msgId;
	private String reqMapping;
	private String rs;
	private String client;
	private String  server;
	private Integer code;
	private String err;
	private Object[] args; 
	private String rsClass;
	
	public <T> MqRpcData setResult(T rs) {
		this.rs = JSON.toJSONString(rs);
		rsClass = rs.getClass().getName();
		args = null;
		reqMapping = null;
		return this;
	}
	public MqRpcData setCode(int code,  String err) {
		this.code = code;
		this.err = err;
		return this;
	}
	public int getCode() {
		if(code == null) {
			return 0;
		}
		return code;
	}
	public static MqRpcData build(long msgId, String reqMapping, Object client, Object server, Object[] args){
		MqRpcData  entity = new MqRpcData();
		entity.msgId = msgId;
		entity.reqMapping = reqMapping;
		entity.args = args;	
		entity.client = client.toString();
		entity.server = server.toString();
		return entity;
	}
	
	public long getMsgId() {
		return msgId;
	}
	public String getReqMapping() {
		return reqMapping;
	}
	public String getRs() {
		return rs;
	}
	public String getClient() {
		return client;
	}
	public String getServer() {
		return server;
	}
	public String getErr() {
		return err;
	}
	public Object[] getArgs() {
		return args;
	}
	public String getRsClass() {
		return rsClass;
	}
}
