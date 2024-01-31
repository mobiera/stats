package com.mobiera.ms.commons.stats.svc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import com.mobiera.commons.api.RegisterRequest;
import com.mobiera.commons.api.RegisterResponse;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.res.c.RegisterClientResource;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

@Singleton
public class StatController {

	
	
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.in.memory.past.units")
	Integer inMemoryPastUnits;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.sleep.per.flush.millis")
	Long sleepPerFlushMillis;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.timezone")
	String timezoneName;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.debug")
	Boolean debug;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.standalone")
	Boolean standalone;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.threads")
	Integer threads;
	
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.retry.delay")
	Long retryDelay;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.ex.delay")
	Long exDelay;
	
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.queue.name")
	String queueName;
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.jms.incoming.ttl")
	Long ttl;
	
	@ConfigProperty(name = "com.mobiera.ms.mno.app.instance_id")
	String instanceId;
	
	
	@Inject ConnectionFactory connectionFactory;
	
	
	@Inject
	StatBuilderService statService;
	
	@RestClient
	@Inject
	RegisterClientResource registerClientResource;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.app.name")
	String appName;
	
	@Inject EntityManager em;
	
	private String appId = null;
	private String registryId = null;
	private ZoneId tz = null;
	
	private Object purgeStackLock = new Object();
	
	private static final Logger logger = Logger.getLogger("StatController");

	private static boolean startedPurge = true;
	
	private boolean finished = false;
	
	void onStart(@Observes StartupEvent ev) {
		logger.info("onStart: starting");
		//startStatConsumers();
		
		startPurgeTask();
		statService.setStartedService(true);
		
		logger.info("onStart: started");
    }
	
	void onStop(@Observes ShutdownEvent ev) {               
		logger.info("onStop: starting index creation");
		createIndexes();
		//stopStatConsumers();
		stopPurgeTask();
		statService.setStartedService(false);
		
		logger.info("onStop: index created, stopped");
	}

	
	void createIndexes() {
    	// create required indexes
		
		
		InputStream inputStream = getClass().getResourceAsStream("/indexes.sql");
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		
		String line;
	    try {
			while( (line = br.readLine()) != null )
			{
				if (!line.strip().isEmpty()) {
					if (!line.startsWith("--")) {
						executeIndexQuery(line);
					}
				}
				
			}
		} catch (IOException e) {
			logger.error("", e);
		}
		
		
    	
    }

	@Transactional
	public void executeIndexQuery(String line) {
		logger.info("executeIndexQuery: query: " + line);
		em.createNativeQuery(line).executeUpdate();
	}

	public void startPurgeTask() {
		Uni.createFrom().item(UUID::randomUUID).emitOn(Infrastructure.getDefaultWorkerPool()).subscribe().with(
                this::startPurgeTask, Throwable::printStackTrace
        );
	}

	private Uni<Void> startPurgeTask(UUID uuid) {
		
		if (standalone) {
			appId = appName;
			tz = ZoneId.of(timezoneName);
		} else {
			Object lockObj = new Object();
			RegisterRequest re = new RegisterRequest();
			re.setApp("STATS");
			re.setId("stats" + instanceId);
			while (appId == null) {
				try {
					logger.info("startPurgeTask: " + JsonUtil.serialize(re, false));
					RegisterResponse response = registerClientResource.registerRequest(re);
					appId = response.getId();
					registryId = response.getRegistryId();
				} catch (Exception e) {
					logger.error("startPurgeTask: cannot register yet, will retry: ", e);
					synchronized(lockObj) {
						try {
							lockObj.wait(5000l);
						} catch (InterruptedException e1) {
							
						}
					}
				}
			}
			
		}
		
		
		if (tz == null) {
			try {
				tz = ZoneId.of(timezoneName);
			} catch (Exception e) {
				logger.error("getZoneId: invalid com.mobiera.ms.commons.stats.timezone: " + timezoneName);
				throw(e);
			}

		}

		
		logger.info("configured timezone: " + tz + " appId: " + appId + " registryId: " + registryId);
		
		statService.init(tz);
		startedPurge = true;
		
		
		
		logger.info("startPurgeTask: " + uuid);
		
		
		synchronized (purgeStackLock) {
			try{
				logger.info("startPurgeTask: sleeping...");
				purgeStackLock.wait(sleepPerFlushMillis);
			} catch(InterruptedException e){
				logger.error("startPurgeTask: interrupted");
			}

		}
		logger.info("startPurgeTask: sleeping... done");
		
		
		while (startedPurge) {
			try {
				//if (debug) {
					logger.info("startPurgeTask: flushStat");
				//}
				statService.flushStats(false);
				
						synchronized (purgeStackLock) {
							try{
								purgeStackLock.wait(sleepPerFlushMillis);
							} catch(InterruptedException e){
								logger.error("startPurgeTask: interrupted");
							}

						}
					}
				
			catch (Exception e) {
				
				logger.error("startPurgeTask: non fatal error", e);
				synchronized (purgeStackLock) {
					try{
						purgeStackLock.wait(sleepPerFlushMillis);
					} catch(InterruptedException e1){
						logger.error("startPurgeTask: Exception ", e1);
					}

				}
			}
			
		
		}
		
		
		try {
			statService.flushStats(true);
		} catch (Exception e) {
			
			logger.error("startPurgeTask: non fatal error", e);
		}
			
		logger.info("startPurgeTask: exiting " + uuid);
		
		finished = true;
		
		return Uni.createFrom().voidItem();
    
		
	}
	
	
	public void stopPurgeTask() {
		startedPurge = false;
		synchronized (purgeStackLock) {
			purgeStackLock.notifyAll();
		}
		logger.warn("stopPurgeTask: notified");
		
		Object lockObj = new Object();
		
		logger.info("stopPurgeTask: finished: " + finished);
		while (!finished) {
			synchronized (lockObj) {
				try {
					lockObj.wait(1000l);
					logger.info("stopPurgeTask: waiting for flush to finish...");
				} catch (Exception e) {
					
				}
			}
		}
	}
	
	public String getAppId() {
		return appId;
	}

	public String getRegistryId() {
		return registryId;
	}

	/*
	private static List<StatConsumer> consumers = new ArrayList<StatConsumer>();
	
	private void stopStatConsumers() {
		for (StatConsumer c: consumers) {
			c.stop();	
		}
	}
	
	public void startStatConsumers() {
		
		for (int i=0; i<threads;i++) {
			logger.info("startStatConsumers: starting consumer #" + i);
			StatConsumer consumer = new StatConsumer(connectionFactory, exDelay, queueName, i, debug);
			consumer.start();	
			consumers.add(consumer);
		}
		
		
	}

	*/

}
