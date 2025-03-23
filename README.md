# STATS Project Documentation
The STATS project, developed by `MOBIERA SAS`, is designed to handle statistics in conjunction with dependencies like STATS-API and mobiera-commons. Importing these dependencies is explained below. The STATS module manages statistics, STATS-API acts as an interface between the service and the statistics module, and mobiera-commons provides general components.

## Configuration in application.properties
Ensure the following properties in the `application.properties` file are updated for the STATS project:

```
com.mobiera.ms.commons.stats.standalone=true - Set to true since the library is part of another project and works as an independent module
com.mobiera.ms.commons.stats.threads=32 - The number of threads; consider resource consumption
quarkus.datasource.db-kind - Database kind, preferably postgres
quarkus.datasource.username - Database username
quarkus.datasource.password - Database password
quarkus.datasource.jdbc.url - Database URL
com.mobiera.ms.commons.stats.debug - Set to true to activate logging
com.mobiera.ms.commons.stats.jms.queue.name - set the queue name
```

## Artemis configuration


You can configure until 8 Artemis servers for queues. You need to decide globally for your project and all mno-adapters deployed for this project MUST share the same configuration.

All queues are replicated across all instances.

**Note**: Ensure that the Artemis credentials in your project can connect with the configured credentials.

### Define the number of required instances

Select from 1 to 8.

```
com.mobiera.ms.mno.quarkus.artemis.instances=1
```

### Configure each instance

```
quarkus.artemis."a0".url=tcp://artemis:61616
quarkus.artemis."a0".username=quarkus
quarkus.artemis."a0".password=...

quarkus.artemis."a1".url=tcp://artemis:61616
quarkus.artemis."a1".username=quarkus
quarkus.artemis."a1".password=...

quarkus.artemis."a2".url=tcp://artemis:61616
quarkus.artemis."a2".username=quarkus
quarkus.artemis."a2".password=...

quarkus.artemis."a3".url=tcp://artemis:61616
quarkus.artemis."a3".username=quarkus
quarkus.artemis."a3".password=...

quarkus.artemis."a4".url=tcp://artemis:61616
quarkus.artemis."a4".username=quarkus
quarkus.artemis."a4".password=...

quarkus.artemis."a5".url=tcp://artemis:61616
quarkus.artemis."a5".username=quarkus
quarkus.artemis."a5".password=...

quarkus.artemis."a6".url=tcp://artemis:61616
quarkus.artemis."a6".username=quarkus
quarkus.artemis."a6".password=...

quarkus.artemis."a7".url=tcp://artemis:61616
quarkus.artemis."a7".username=quarkus
quarkus.artemis."a7".password=...

```

### Kubernetes

use:

```
 
 COM_MOBIERA_MS_MNO_QUARKUS_ARTEMIS_INSTANCES=...

 QUARKUS_ARTEMIS__A0__URL=...
 QUARKUS_ARTEMIS__A0__USERNAME=...
 QUARKUS_ARTEMIS__A0__PASSWORD=...
 
 QUARKUS_ARTEMIS__A1__URL=...
 ...
 
```



## Usage
The STATS project uses Swagger for easy exploration of available endpoints for statistic queries. It is recommended to use a REST client on port 8700 (default) for the following endpoints:
- stats/view: View statistics; can return cumulative values for better interpretation.
- stats/compare: Compare statistics.
- stats/sumLastNStatVO: Sum of statistics over a time interval.
- stats/get: Get statistics classes by entity type.

## Token Authorization
To access the STATS-API library from the specified repositories, you need to ensure that the token associated with the `gitlab-maven-microservice-commons-group` matches the one generated for the STATS-API project. Additionally, make sure you have the token for the `gitlab-maven-mobiera-commons-group` project, as the STATS API relies on an existing library from that project. This token can be created either in the `settings.xml` file or as an environment variable for improved security.

## Connection Classes for Artemis
Two suggested classes for handling the connection with the Artemis module:

- AbstractProducer
```java
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Session;

import org.graalvm.collections.Pair;
import org.jboss.logging.Logger;


public class AbstractProducer {
	
	private Integer producerId = 0;
	private Integer producerCount = 8;
	
    private Map<Integer,JMSProducer> producers = new HashMap<Integer,JMSProducer>();
    private Map<Integer,JMSContext> contexts = new HashMap<Integer,JMSContext>();
    
	private static final Logger logger = Logger.getLogger(AbstractProducer.class);

	private Object contextLockObj = new Object();
	
	protected Pair<Integer, Pair<JMSContext, JMSProducer>> getProducer(ConnectionFactory connectionFactory, boolean debug) {
    	JMSProducer producer = null;
    	JMSContext context = null;
    	int id = 0;
    	
    	synchronized (contextLockObj) {
			if (debug) {
    			logger.info("spool: with use contexts/producer #" + producerId);
    		}
			context = contexts.get(producerId);
			if (context == null) {
				context = connectionFactory.createContext(Session.CLIENT_ACKNOWLEDGE);
				contexts.put(producerId, context);
			}
			
			producer = producers.get(producerId);
			if (producer == null) {
				producer = context.createProducer();
				producers.put(producerId, producer);
			}
			id = producerId;
			producerId++;
			if (producerId == producerCount) {
				producerId = 0;
			}
		}
    	return Pair.create(id, Pair.create(context, producer));
    }
    
    protected void purgeAllProducers() {
    	synchronized (contextLockObj) {
    		for (int id=0; id<contexts.size(); id++) {
    			JMSContext context = contexts.get(id);
        		if (context != null) {
        			try {
        	   		 	context.close();
        	   		 	logger.info("purgeAllProducers: closed producer #" + id);
     		         } catch (Exception e1) {
     		         	logger.error("purgeProducer: error closing producer #" + id, e1);
     		         }
        		}
        		contexts.remove(id);
        		producers.remove(id);
    		}
    		
    		logger.info("purgeAllProducers: remaining contexts size: " + contexts.size() + " producer size: " + producers.size());

    		contexts.clear();
    		producers.clear();
    		
    		logger.info("purgeAllProducers: cleared contexts size: " + contexts.size() + " producer size: " + producers.size());
    		
    	}
    }

	public void setProducerCount(Integer producerCount) {
		this.producerCount = producerCount;
	}
}
```

- StatProducer
```java
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;

import org.graalvm.collections.Pair;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mobiera.chatapp.chatbackend.util.ServiceConfig;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.CommonStatEnum;
import com.mobiera.ms.commons.stats.api.StatEnum;
import com.mobiera.ms.commons.stats.api.StatEvent;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class StatProducer extends AbstractProducer {

	@Inject
    ConnectionFactory connectionFactory;

	private static final Logger logger = Logger.getLogger(StatProducer.class);
	
	int id = 0;
    
    public boolean isDebugEnabled() {
			return ServiceConfig.DEBUG;
	}
    
    void onStart(@Observes StartupEvent ev) {
    	logger.info("onStart: queue name " + ServiceConfig.QUEUENAME);
    	this.setProducerCount(ServiceConfig.POOLSIZE);
    }

    void onStop(@Observes ShutdownEvent ev) {
    }
 
    public void spool(String statClass, 
    		String smppAccountId, 
    		List<StatEnum> enums, 
    		Instant ts, 
    		Integer increment) throws JMSException {
    	
    	List<StatEnum> statEnums = new ArrayList<StatEnum>(enums.size());
    	for (StatEnum se: enums) {
    		statEnums.add(CommonStatEnum.build(se.getIndex(), se.getValue()));
    	}
    	StatEvent event = new StatEvent();
    	event.setEntityId(smppAccountId);
    	event.setEnums(statEnums);
    	event.setIncrement(increment);
    	event.setTs(ts);
    	event.setStatClass(statClass);
    	
    	if ((enums == null)
    			|| (enums.size() == 0)
    			|| ( smppAccountId == null)
    			|| (ts == null)
    			|| (increment == null)
    			|| (statClass == null)
    			) {
    		try {
				logger.warn("spool: ignoring, check attributes: " + JsonUtil.serialize(event, false));
			} catch (JsonProcessingException e) {
			}
    	} else {
    		this.spool(event);
    	}
    }
    
    public void spool(
    		String statClass, 
    		String smppAccountId, 
    		StatEnum statEnum, 
    		Instant ts, 
    		Integer increment, Double doubleIncrement) throws JMSException {
    	
    	StatEvent event = new StatEvent();
    	event.setEntityId(smppAccountId);
    	List<StatEnum> statEnums = new ArrayList<StatEnum>(1);
    	statEnums.add(CommonStatEnum.build(statEnum.getIndex(), statEnum.getValue()));
    	event.setEnums(statEnums);
    	event.setIncrement(increment);
    	event.setDoubleIncrement(doubleIncrement);
    	event.setTs(ts);
    	event.setStatClass(statClass);
    	
    	if ((statEnums == null)
    			|| (statEnums.size() == 0)
    			|| ( smppAccountId == null)
    			|| (ts == null)
    			|| (increment == null)
    			|| (statClass == null)
    			) {
    		try {
				logger.warn("spool: ignoring, check attributes: " + JsonUtil.serialize(event, false));
			} catch (JsonProcessingException e) {
	  		}
    	} else {
    		this.spool(event);
      	}
    }
    
    public void spool(StatEvent event) throws JMSException {
    	this.spool(event, 0);
    }
    public void spool(StatEvent event, int attempt) throws JMSException {
    	
    	JMSProducer producer = null;
    	JMSContext context = null;
    	boolean retry = false;
    	
    	try {
    		Pair<Integer, Pair<JMSContext, JMSProducer>> jms = getProducer(connectionFactory, isDebugEnabled());
    		
    		producer = jms.getRight().getRight();
    		context = jms.getRight().getLeft();
    		id = jms.getLeft();
    		ObjectMessage message = context.createObjectMessage(event);
        	synchronized (producer) {
        		Queue queue = this.getQueue(context);
        		producer.send(queue, message);
            	message.acknowledge();
        	}
        	
        	if (isDebugEnabled()) {
        		try {
    				logger.info("spool: stat event spooled to " + ServiceConfig.QUEUENAME + ":" + JsonUtil.serialize(event, false));
    			} catch (JsonProcessingException e) {
    				logger.error("", e);
      			}
        	}
    	} catch (Exception e) {
    		this.purgeAllProducers();
   			logger.error("error", e);
 			attempt++;
 			if (attempt<ServiceConfig.POOLSIZE) {
 				logger.info("spool: will retry attempt #" + attempt);
 				retry = true;
 			} else {
 				throw (e);
 	  		}
    	}
    	if (retry) this.spool(event, attempt);
    }

  private Queue queue = null;
	private Queue getQueue(JMSContext context) {
		if (queue == null) {
			queue = context.createQueue(ServiceConfig.QUEUENAME);
		}
		return queue;
	}
}
```


## Sending Statistics to Accumulate
To send statistics for accumulation, use the following parameters:
```java
statProducer.spool(
	FriendChatClass.ANIMATOR.toString(),
	entityId, 
	AnimatorStat.SENT_MSGS, 
	Instant.now(), 
	1);
```
Here, `FriendChatClass.ANIMATOR` represents the type of statistics class, `entityId` is the associated entity (preferably a UUID), `AnimatorStat.SENT_MSGS` is the specific statistic to accumulate, `Instant.now()` is the timestamp, and `1` is the increment.
