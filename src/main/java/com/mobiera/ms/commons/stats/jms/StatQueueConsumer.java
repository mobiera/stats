package com.mobiera.ms.commons.stats.jms;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.StatEvent;
import com.mobiera.ms.commons.stats.svc.StatBuilderService;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;

@ApplicationScoped
public class StatQueueConsumer {

	@Inject
    ConnectionFactory connectionFactory;

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

    
    
    void onStart(@Observes StartupEvent ev) {
    	//scheduler.submit(this);
    	
    	for (int i=0; i<threads;i++) {
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
     				context = connectionFactory.createContext(Session.SESSION_TRANSACTED);
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
        								//if (debug) 
        								logger.info("statConsumer: " + queueName + " after rollback "+ uuid + " " + (System.currentTimeMillis() - now));

        							} 
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
	
	
	
	
    
   
}