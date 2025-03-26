package com.mobiera.ms.commons.stats.jms;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import jakarta.jms.ConnectionFactory;

public abstract class MultiAbstractConsumer {

	/*
     * Artemis configurations
     */
	
	
    private List<ConnectionFactory> connectionFactories;
    private Object artemisConfigLockobj = new Object();
    private Integer connectionFactoryIdx = 0;
    
    private static final Logger logger = Logger.getLogger(MultiAbstractConsumer.class);
	
	protected void configure(
			ConnectionFactory connectionFactory0,
			ConnectionFactory connectionFactory1,
			ConnectionFactory connectionFactory2,
			ConnectionFactory connectionFactory3,
			ConnectionFactory connectionFactory4,
			ConnectionFactory connectionFactory5,
			ConnectionFactory connectionFactory6,
			ConnectionFactory connectionFactory7
			) {
		
		Integer instanceCount = ConfigProvider.getConfig().getValue("com.mobiera.ms.mno.quarkus.artemis.instances", Integer.class);
		
		synchronized (artemisConfigLockobj) {
			
			
			connectionFactories = new ArrayList<ConnectionFactory>();
			connectionFactories.add(connectionFactory0);
			if (instanceCount>1) connectionFactories.add(connectionFactory1);
			if (instanceCount>2) connectionFactories.add(connectionFactory2);
			if (instanceCount>3) connectionFactories.add(connectionFactory3);
			if (instanceCount>4) connectionFactories.add(connectionFactory4);
			if (instanceCount>5) connectionFactories.add(connectionFactory5);
			if (instanceCount>6) connectionFactories.add(connectionFactory6);
			if (instanceCount>7) connectionFactories.add(connectionFactory7);
			
			logger.info(connectionFactory0);
			logger.info("using instanceCount: " + instanceCount + " artemis configuration(s). You can override by setting com.mobiera.ms.mno.quarkus.artemis.instances and configuring artemis URLs and credentials. refer to README.md of mno-adapters-rest-endpoint for manual");

		}
		
		
	}
	
	

	private static Object getConnectionFactoryLockObj = new Object();
	
	protected ConnectionFactory getConnectionFactory() {
		ConnectionFactory c = null;
		synchronized (getConnectionFactoryLockObj) {
			c = connectionFactories.get(connectionFactoryIdx);
			connectionFactoryIdx++;
			if (connectionFactoryIdx>=connectionFactories.size()) {
				connectionFactoryIdx = 0;
			}
		}
		
		return c;
	}
	
	protected int getConnectionFactoriesSize() {
		return connectionFactories.size();
	}
	
	
}
