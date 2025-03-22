package com.mobiera.ms.commons.stats.jms;


import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.StatEnum;
import com.mobiera.ms.commons.stats.api.StatEvent;
import com.mobiera.ms.commons.stats.svc.StatBuilderService;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import lombok.Data;

@ApplicationScoped
public class StatQueueConsumer extends MultiAbstractConsumer {

	@Inject
	StatBuilderService statBuilderService;
	
	
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.retry.delay")
	Long retryDelay;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.ex.delay")
	Long exDelay;
	
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.queue.name")
	String queueName;
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.incoming.ttl")
	Long ttl;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.debug")
	Boolean debug;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.threads")
	Integer threads;
	
	
	
	
	private static final Logger logger = Logger.getLogger(StatQueueConsumer.class);
	private Map<UUID,Object> lockObjs = new HashMap<UUID,Object>();
	private Map<UUID,Boolean> runnings = new HashMap<UUID,Boolean>();
	private Map<UUID,Boolean> starteds = new HashMap<UUID,Boolean>();

	/*
     * Artemis configurations
     */
    
	public StatQueueConsumer() { super();}
	
	public StatQueueConsumer(@Identifier("a0") ConnectionFactory connectionFactory0,
			@Identifier("a1") ConnectionFactory connectionFactory1,
			@Identifier("a2") ConnectionFactory connectionFactory2,
			@Identifier("a3") ConnectionFactory connectionFactory3,
			@Identifier("a4") ConnectionFactory connectionFactory4,
			@Identifier("a5") ConnectionFactory connectionFactory5,
			@Identifier("a6") ConnectionFactory connectionFactory6,
			@Identifier("a7") ConnectionFactory connectionFactory7
			) {
		super(connectionFactory0, connectionFactory1, connectionFactory2, connectionFactory3, connectionFactory4,
				connectionFactory5, connectionFactory6, connectionFactory7);
	}
    
    
    void onStart(@Observes StartupEvent ev) {
    	//scheduler.submit(this);
    	
    	int totalThreads = threads * this.getConnectionFactoriesSize();
    	
    	for (int i=0; i<totalThreads;i++) {
			logger.info("startStatConsumers: starting consumer #" + i);
			UUID uuid = UUID.randomUUID();
			starteds.put(uuid, true);
			startConsumer(uuid);
			
		}
    	
    }

    void onStop(@Observes ShutdownEvent ev) {
    	
    	for (UUID uuid: lockObjs.keySet()) {
    		stopConsumer(uuid);
    	}
    	
    }
 
    
    private static Object shutdownLockObj = new Object();
    
	public void stopConsumer(UUID uuid) {
		
		synchronized(starteds) {
			starteds.put(uuid, false);
		}
		
		
		Object lockObj = lockObjs.get(uuid);
		
		if (lockObj != null) {
			synchronized (lockObj) {
				lockObj.notifyAll();
			}
		}
		
		while (true) {
			
			Boolean running = null;
			synchronized (runnings) {
				running = runnings.get(uuid);
			}
			if (!running) {
				break;
			
			}
			synchronized (shutdownLockObj) {
				try {
					shutdownLockObj.wait(100);
				} catch (InterruptedException e) {
					
				}
				synchronized (lockObj) {
					lockObj.notifyAll();
				}
			}
		}
		
		
		logger.info("stopConsumer: stopped: " + uuid);
		
	}
    
    Uni<Void> runConsumer(UUID uuid) {
    	
    	JMSContext context = null;
        Queue queue = null;
        
    	Object lockObj = new Object();
    	synchronized (lockObjs) {
    		lockObjs.put(uuid, lockObj);
    	}
    	synchronized (runnings) {
    		runnings.put(uuid, true);
    	}
    	
    	
    	long now = System.currentTimeMillis();
    	synchronized (lockObj) {
			try {
				lockObj.wait(10000l);
			} catch (InterruptedException e) {
				
			}
		}
    	
    	
    	 while (true) {
    		 Boolean started = null;
    		 synchronized(starteds) { 
    			 started = starteds.get(uuid);
    			 
    		 }
    		 if ((started == null) || (!started)) {
    			 break;
    		 }
    		 
    		 
     		logger.info("run: running " + uuid);

    		 try  {
    			 
    			 if (debug) logger.warn("statConsumer " + queueName + ": create session " + uuid );


     			if (context == null) {
     				context = getConnectionFactory().createContext(Session.SESSION_TRANSACTED);
     			}


     			if (debug) logger.warn("statConsumer " + queueName + ": session created " + uuid );

     			if (queue == null) {
     				queue = context.createQueue(queueName);
     			}
     			if (debug) logger.warn("statConsumer " + queueName + ": create consumer " + uuid );
     			JMSConsumer consumer = context.createConsumer(queue);

     			if (debug) logger.info("statConsumer " + queueName + ": waiting for message... " + uuid);

    			 
     			while (true) {
	     	    		 started = null;
	     	    		 synchronized(starteds) { 
	     	    			 started = starteds.get(uuid);
	     	    			 
	     	    		 }
	     	    		 if ((started == null) || (!started)) {
	     	    			 break;
	     	    		 }
    			 
    	           
    	            	
    	            	//if (logger.isInfoEnabled()) 
    	            	//logger.warn("run: stat builder service ready?");
    	            	
    	            	now = System.currentTimeMillis();
    	            	
    	            	if (statBuilderService.isStartedService()) {
    	            		
    	            		
    	            		
    	            		if (debug) 
    	            			logger.info("statConsumer: waiting for message... " + uuid + " " + (System.currentTimeMillis() - now));
        	            	
    	            		Message message = consumer.receiveNoWait();
    	            		
        	                if (message != null) {
        	                	if (debug) 
        	                		logger.info("statConsumer: received message " + uuid + " " + (System.currentTimeMillis() - now));
            	            	
        	                	
        	                	
        	                	StatEvent event = null;
        						if (message instanceof ObjectMessage) {

        							ObjectMessage objMsg = (ObjectMessage) message;
        							event = (StatEvent) objMsg.getObject();
        							
        							if (debug) {
            	                		try {
    										logger.info(JsonUtil.serialize(objMsg.getObject(), false));
    									} catch (JsonProcessingException e2) {
    										
    									}
            	                	}
											processStatEvent(uuid, now, event, context);
        						} else if (message instanceof TextMessage) {
											ObjectMapper objectMapper = new ObjectMapper();
											TextMessage txtMsg = (TextMessage) message;
											JsonNode rootNode = objectMapper.readTree(txtMsg.getText());
											
											List<StatEnum> enums = new ArrayList<>();
											rootNode.path("enums").forEach(enumNode -> {
													try {
															enums.add(objectMapper.treeToValue(enumNode, StatEnumImpl.class));
        								} catch (JsonProcessingException e) {
													}
											});
											event = new StatEvent();
											event.setStatClass(rootNode.path("statClass").asText("null"));
											event.setEntityId(rootNode.path("entityId").asText("null"));
											if (!rootNode.path("ts").isMissingNode()) {
													try {
															event.setTs(Instant.parse(rootNode.path("ts").asText()));
													} catch (DateTimeParseException e) {
													}
											}
											event.setIncrement(rootNode.path("increment").asInt(0));
											event.setDoubleIncrement(rootNode.path("doubleIncrement").asDouble(0.0));
											event.setEnums(enums);
											processStatEvent(uuid, now, event, context);
        						} else {
        							if (debug) logger.warn("statConsumer " + queueName + " "+ uuid + " " + (System.currentTimeMillis() - now)+ ": unkown event " + event);
        							context.commit();
        						}
        	                	
        	                	
        	                }  else {
        	                	if (debug) 
        	                		logger.info("statConsumer: no delivered message " + uuid + " " + (System.currentTimeMillis() - now));
            	            	
        						synchronized (lockObj) {
        							try {
        								if (debug) logger.info("statConsumer: waiting thread " + uuid + " " + (System.currentTimeMillis() - now));
        								lockObj.wait(1000l);
        							} catch (InterruptedException e1) {
        							}
        						}
        					}
    	            	} else {
    	            		logger.warn("statConsumer: waiting for stat builder service to be ready");
        	            	
    	            		synchronized (lockObj) {
    	            			try {
									lockObj.wait(exDelay);
								} catch (InterruptedException e) {
									
								}
    	            		}
    	            	}
    	            	
    	                
    	            }
    	            consumer.close();
        			context.close();
        			consumer = null;
        			context = null;
    	        } catch (Exception e) {
    	        	try {
        				
        				context.close();
        			} catch (Exception e1) {

        			}
        			context = null;
        			queue = null;



        			synchronized (lockObj) {
        				try {
        					lockObj.wait(exDelay);
        				} catch (InterruptedException e1) {
        				}
        			}
    	        }
    	 }
    	 synchronized (runnings) {
    		 runnings.put(uuid, false);
    	 }
    	
    	 return Uni.createFrom().voidItem();
    }
    
    
    public void startConsumer(UUID uuid) {
		Uni.createFrom().item(uuid).emitOn(Infrastructure.getDefaultWorkerPool()).subscribe().with(
                this::runConsumer, Throwable::printStackTrace
        );
	}

	
    private static StatBuilderService sbs = null;
	private StatBuilderService getStatBuilderService() {
		if (sbs == null) {
			sbs = Arc.container().instance(StatBuilderService.class).get();
		}
		return sbs;
	}
	
	
	private void processStatEvent(UUID uuid, long now, StatEvent event, JMSContext context) {
		try {
			if (debug) 
				logger.info("statConsumer: " + queueName + " before stat "+ uuid + " " + (System.currentTimeMillis() - now));

			getStatBuilderService().statEvent(event);
			if (debug) 
				logger.info("statConsumer: " + queueName + " after stat, before commit "+ uuid + " " + (System.currentTimeMillis() - now));

			context.commit();
			if (debug) 
				logger.info("statConsumer: " + queueName + " after commit "+ uuid + " " + (System.currentTimeMillis() - now));
		} catch (Exception e) {
			try {
				logger.warn("statConsumer: " + queueName + " "+ uuid + " " + (System.currentTimeMillis() - now)+ ": exception " + JsonUtil.serialize(event, false), e);
			} catch (JsonProcessingException e1) {
				logger.warn("statConsumer: " + queueName + " "+ uuid + " " + (System.currentTimeMillis() - now)+ ": exception", e);
			}
			context.rollback();
			logger.info("statConsumer: " + queueName + " after rollback "+ uuid + " " + (System.currentTimeMillis() - now));
		}
	}
	
	@Data
	public static class StatEnumImpl implements StatEnum {
    private Integer index;
    private String label;
    private String value;
    private String description;
	}
	
    
   
}