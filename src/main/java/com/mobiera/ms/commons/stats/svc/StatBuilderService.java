package com.mobiera.ms.commons.stats.svc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.FlushModeType;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.StatEnum;
import com.mobiera.ms.commons.stats.api.StatEvent;
import com.mobiera.ms.commons.stats.api.StatGranularity;
import com.mobiera.ms.commons.stats.api.StatVO;
import com.mobiera.ms.commons.stats.model.Stat;

@ApplicationScoped
public class StatBuilderService {

	@Inject
	EntityManager em;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.in.memory.past.units")
	Integer inMemoryPastUnits;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.sleep.per.flush.millis")
	Long sleepPerFlushMillis;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.timezone")
	String timezoneName;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.debug")
	Boolean debug;
	
	@ConfigProperty(name = "com.mobiera.ms.mno.app.instance_id")
	String instanceId;
	
	
	private ZoneId tz = null;
	
	private static Logger logger = Logger.getLogger(StatBuilderService.class);
	private static Map<String, Map<String, Map<StatGranularity, Map<Instant, Stat>>>> stats;
	private static boolean startedService = false;
	
	
	public boolean isDebugEnabled() {
		return (logger.isInfoEnabled() && debug);
	}
	
	
	
	
	
	
	
	
	public void stat(String statClass, String entityId, StatEnum e, Instant ts, int increment, double doubleIncrement) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		
		
		if (isDebugEnabled()) {
			logger.info("stat: statClass: " + statClass + " entityId: " + entityId + " stats.keySet().size(): " + stats.keySet().size() );
		}
		
		
		Stat hourlyStat = this.getStat(statClass, entityId, StatGranularity.HOUR, this.getHourTs(ts), true);
		Stat dailyStat = this.getStat(statClass, entityId, StatGranularity.DAY, this.getDayTs(ts), true);
		Stat monthlyStat = this.getStat(statClass, entityId, StatGranularity.MONTH, this.getMonthTs(ts), true);
		
		
		privateStat(statClass, entityId, e, ts, increment, doubleIncrement, hourlyStat, dailyStat, monthlyStat);
			
		
		
		
	}
	
	public void stat(String statClass, String entityId, List<StatEnum> enums, Instant ts, int increment, double doubleIncrement) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		
		if (isDebugEnabled()) {
			logger.info("stat: statClass: " 
		+ statClass + " entityId: " + entityId + " stats.keySet().size(): " 
					+ stats.keySet().size()
					+ " increment: " + increment + " doubleIncrement: " + doubleIncrement
					);
		}
		
		
		Stat hourlyStat = this.getStat(statClass, entityId, StatGranularity.HOUR, this.getHourTs(ts), true);
		Stat dailyStat = this.getStat(statClass, entityId, StatGranularity.DAY, this.getDayTs(ts), true);
		Stat monthlyStat = this.getStat(statClass, entityId, StatGranularity.MONTH, this.getMonthTs(ts), true);
		
		
		for(StatEnum e:enums) {
			
			
			privateStat(statClass, entityId, e, ts, increment, doubleIncrement, hourlyStat, dailyStat, monthlyStat);
			
			
		}
		
		
		
	}
	
	/*public void statDouble(String statClass, Long entityId, List<StatEnum> enums, Instant ts, double doubleIncrement) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		
		if (isDebugEnabled()) {
			logger.info("stat: statClass: " + statClass + " entityId: " + entityId + " stats.keySet().size(): " + stats.keySet().size() );
		}
		
		
		Stat hourlyStat = this.getStat(statClass, entityId, StatGranularity.HOUR, this.getHourTs(ts), true);
		Stat dailyStat = this.getStat(statClass, entityId, StatGranularity.DAY, this.getDayTs(ts), true);
		Stat monthlyStat = this.getStat(statClass, entityId, StatGranularity.MONTH, this.getMonthTs(ts), true);
		
		
		for(StatEnum e:enums) {
			
			
			privateStatDouble(statClass, entityId, e, ts, doubleIncrement, hourlyStat, dailyStat, monthlyStat);
			
			
		}
		
		
		
	}*/

	public void privateStat(String statClass, String entityId, StatEnum e, Instant ts, int increment,
			double doubleIncrement,
			Stat hourlyStat,
			Stat dailyStat, Stat monthlyStat) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (isDebugEnabled()) {
			logger.info("stat: statClass: " + statClass + " entityId: " + entityId + " dateStat: " + ts + " enumStats - hourlyStat - e/v: " + e + "/" + hourlyStat + " " + e + " increment " + increment + " doubleIncrement " + doubleIncrement);
		}
		
		if (e.getIndex()<128) {
			if (isDebugEnabled()) {
				logger.info("stat: e.getIndex()<128 statClass: " + statClass + " entityId: " + entityId + " dateStat: " + ts + " enumStats - hourlyStat - e/v: " + e + "/" + hourlyStat + " " + e + " increment " + increment + " doubleIncrement " + doubleIncrement);
			}
			Method getMethod = Stat.class.getMethod("getLong" + e.getIndex());
			Method setMethod = Stat.class.getMethod("setLong" + e.getIndex(), Long.class);
			Object lockObj = null;
			
			lockObj = this.getLockObj(hourlyStat.getLockObjId());
			synchronized (lockObj) {
				Long hourlyValue = (Long) getMethod.invoke(hourlyStat);
				if (hourlyValue == null) hourlyValue = 0l;
				hourlyValue +=  increment;
				setMethod.invoke(hourlyStat, hourlyValue);
				
			}
			lockObj = this.getLockObj(dailyStat.getLockObjId());
			synchronized (lockObj) {
				Long dailyValue = (Long) getMethod.invoke(dailyStat);
				if (dailyValue == null) dailyValue = 0l;
				dailyValue +=  increment;
				setMethod.invoke(dailyStat, dailyValue);
				
			}
			lockObj = this.getLockObj(monthlyStat.getLockObjId());
			synchronized (lockObj) {
				Long monthlyValue = (Long) getMethod.invoke(monthlyStat);
				if (monthlyValue == null) monthlyValue = 0l;
				monthlyValue +=  increment;
				setMethod.invoke(monthlyStat, monthlyValue);
			}
		} else {
			if (isDebugEnabled()) {
				logger.info("stat: e.getIndex()>=128 statClass: " + statClass + " entityId: " + entityId + " dateStat: " + ts + " enumStats - hourlyStat - e/v: " + e + "/" + hourlyStat + " " + e + " increment " + increment + " doubleIncrement " + doubleIncrement);
			}
			// double 
			Method getMethod = Stat.class.getMethod("getDouble" + e.getIndex());
			Method setMethod = Stat.class.getMethod("setDouble" + e.getIndex(), Double.class);
			Object lockObj = null;
			
			lockObj = this.getLockObj(hourlyStat.getLockObjId());
			synchronized (lockObj) {
				Double hourlyValue = (Double) getMethod.invoke(hourlyStat);
				if (hourlyValue == null) hourlyValue = 0d;
				hourlyValue +=  doubleIncrement;
				setMethod.invoke(hourlyStat, hourlyValue);
				
			}
			lockObj = this.getLockObj(dailyStat.getLockObjId());
			synchronized (lockObj) {
				Double dailyValue = (Double) getMethod.invoke(dailyStat);
				if (dailyValue == null) dailyValue = 0d;
				dailyValue +=  doubleIncrement;
				setMethod.invoke(dailyStat, dailyValue);
				
			}
			lockObj = this.getLockObj(monthlyStat.getLockObjId());
			synchronized (lockObj) {
				Double monthlyValue = (Double) getMethod.invoke(monthlyStat);
				if (monthlyValue == null) monthlyValue = 0d;
				monthlyValue +=  doubleIncrement;
				setMethod.invoke(monthlyStat, monthlyValue);
			}
		}
		
		
		
	}

	
	
	
	public String getLockObjId(Long entityId, StatGranularity statGranularity, Instant ts, String sc) {
		
		String	lockObjId = "s" + sc + "-" + entityId + "-" + statGranularity.getIndex() + "-" + ts.getEpochSecond();
		return lockObjId;
	}

	
	
	
	public synchronized Stat loadOrCreate(String entityId, StatGranularity statGranularity, Instant ts, String sc) {
		
		
		String id = ts.getEpochSecond() + "-" + statGranularity.toString()+ "-" + entityId + "-" + sc + "-" + instanceId;
		
		Stat stat = em.find(Stat.class, id);
	
		if (stat == null) {
			stat = new Stat();
			stat.setId(id);
			stat.setStatGranularity(statGranularity);
			stat.setTs(ts);
			stat.setEntityId(entityId);
			stat.setStatClass(sc);
		}
		
		
		return stat;
	}

	
	
	public Stat getStat(String statClass, String entityId, StatGranularity statGranularity, Instant currentDateTime, boolean create)  {
		Stat stat = null;
		Instant stateDateTime = null;
		
		
		switch (statGranularity) {
			case HOUR: { stateDateTime = this.getHourTs(currentDateTime); break; }
			case DAY: { stateDateTime = this.getDayTs(currentDateTime); break; }
			case MONTH: { stateDateTime = this.getMonthTs(currentDateTime); break; }
			
		

		}

		Map<String, Map<StatGranularity, Map<Instant, Stat>>> scGanularityStats = null;
		Map<StatGranularity, Map<Instant, Stat>> granularityStats = null;
		Map<Instant, Stat> dateStats = null;

		synchronized (stats) {
			scGanularityStats = stats.get(statClass);
			if ((scGanularityStats == null) && create) {
				scGanularityStats = new ConcurrentHashMap<String, Map<StatGranularity, Map<Instant, Stat>>>(10);
				stats.put(statClass, scGanularityStats);
			}
		}

		if (scGanularityStats != null) {
			synchronized (scGanularityStats) {
				granularityStats = scGanularityStats.get(entityId);
				if ((granularityStats == null) && create) {
					granularityStats = new ConcurrentHashMap<StatGranularity, Map<Instant, Stat>>(10);
					scGanularityStats.put(entityId, granularityStats);
				}
			}
			
		}
		
		if (granularityStats != null) {
			synchronized (granularityStats) {
				dateStats = granularityStats.get(statGranularity);
				if ((dateStats == null) && create) {
					dateStats = new ConcurrentHashMap<Instant, Stat>();
					granularityStats.put(statGranularity, dateStats);
				}
			}
			if (dateStats != null) {
				synchronized (dateStats) {
					stat = dateStats.get(stateDateTime);
					
					
				}
					if (stat == null) {
						if (create) {
							stat = loadOrCreate(entityId, statGranularity, stateDateTime, statClass);
							synchronized (dateStats) {
								
								Stat raceStat = dateStats.get(stateDateTime);
								if (raceStat == null) {
									dateStats.put(stateDateTime, stat);
									if (this.isDebugEnabled()) logger.info("getStat: CREATED: entityId: " + entityId + " stat: " + stat  + " stateDateTime: " + stateDateTime + " statGranularity: " + statGranularity);
								} {
									logger.warn("getStat: Not CREATED: entityId: " + entityId + " stat: " + stat  + " stateDateTime: " + stateDateTime + " statGranularity: " + statGranularity);
								}
								
							}
							
						} else {
							if (this.isDebugEnabled()) logger.info("getStat: NOT EXISTING NOT CREATED: entityId: " + entityId + " stat: " + stat  + " stateDateTime: " + stateDateTime + " statGranularity: " + statGranularity);
						}

					} else {
						if (this.isDebugEnabled()) logger.info("getStat: EXISTS in CACHE: entityId: " + entityId + " stat: " + stat  + " stateDateTime: " + stateDateTime + " statGranularity: " + statGranularity);
					}
				
			}
		}
		return stat;
	}
	

	public boolean isStartedService() {
		return startedService;
	}
	
	
	public void init(ZoneId tz) {
		this.tz = tz;
		
		stats = new ConcurrentHashMap<String, Map<String, Map<StatGranularity, Map<Instant, Stat>>>>(5);
		//em.find(Stat.class, "1");
	}

	@Transactional
	public Stat treatStatFlush(Stat stat) {
		//em.setFlushMode(FlushModeType.COMMIT);
		
		if (isDebugEnabled()) {
			logger.info("treatStatFlush: flushing stat " + stat.getStatClass() + " " + stat.getStatGranularity() + " " + stat.getEntityId() + " " + stat.getTs());
		}
		if (stat.getLastFlushed() == null) {
			stat.setLastFlushed(Instant.now());
			em.persist(stat);
			em.flush();
			

			if (isDebugEnabled()) {
				logger.info("treatStatFlush: persisted productStat");
			}
		} else {
			stat.setLastFlushed(Instant.now());
			stat = em.merge(stat);
			if (isDebugEnabled()) {
				logger.info("treatStatFlush: merged productStat"); 
			}
		}
		return stat;
	}
	
	
	public void flushStats(boolean shutdown)  {
		
		Long now = null;
		if (isDebugEnabled()) {
			now = System.currentTimeMillis();
			logger.info("flushStats: started @" + now);
			logger.info("flushStats: lockObjs size " + this.getLockObjsSize());
		}

		
		for (Iterator<String> m = stats.keySet().iterator(); m.hasNext();) {

			String sc = m.next();
			Map<String, Map<StatGranularity, Map<Instant, Stat>>> scGanularityStats = stats.get(sc);

			if (scGanularityStats != null) {
				if (isDebugEnabled()) {
					logger.info("flushStats: scGanularityStats size: " + scGanularityStats.size() + " sc " + sc);
				}

				for (Iterator<String> i = scGanularityStats.keySet().iterator(); i.hasNext();) {
					String entityId = i.next();
					Map<StatGranularity, Map<Instant, Stat>> granularityStats = scGanularityStats.get(entityId);
					if (granularityStats != null) {
						if (isDebugEnabled()) {
							logger.info("flushStats: granularityStats size: " + granularityStats.size() + " sc " + sc);
						}

						for (Iterator<StatGranularity> j = granularityStats.keySet().iterator(); j.hasNext();) {
							StatGranularity statGranularity = j.next();
							Map<Instant, Stat> dateStats = granularityStats.get(statGranularity);
							if (dateStats != null) {
								if (isDebugEnabled()) {
									logger.info("flushStats: dateStats size: " + dateStats.size() + " sc " + sc);
								}

								Instant lowestAllowedEntryTime = this.getHourTs(Instant.now());
								lowestAllowedEntryTime = lowestAllowedEntryTime.plus(getDurationOffsetForGranularity(statGranularity));

								//synchronized (dateStats) {
									Set<Instant> dateStatsKeySet = dateStats.keySet();
									
									for (Iterator<Instant> k = dateStatsKeySet.iterator(); k.hasNext();) {
										Instant currentDateTime = null;
										synchronized (dateStats) {
											currentDateTime = k.next();
										}
										 
										
										//logger.error("instant: " + currentDateTime + " ds: " + dateStats.hashCode() + " " + statGranularity + " " + System.identityHashCode(dateStats));
										
										Stat stat = null;
										
										synchronized (dateStats) {
											stat = dateStats.get(currentDateTime);
											
										}
										
										if (shutdown) {
											try {
												Object statLockObj = this.getLockObj(stat.getLockObjId());
												synchronized (statLockObj) {
													stat = treatStatFlush(stat);
													
												}
												this.removeLockObj(stat.getLockObjId());
												//em.detach(stat);
											} catch (Exception e) {
												synchronized (dateStats) {
													dateStats.remove(currentDateTime);

												}
											}
											synchronized (dateStats) {
												dateStats.remove(currentDateTime);

											}
										} else if (currentDateTime.isBefore(lowestAllowedEntryTime)) {
											try {
												Object statLockObj = this.getLockObj(stat.getLockObjId());
												synchronized (statLockObj) {
													stat = treatStatFlush(stat);
													
												}
												this.removeLockObj(stat.getLockObjId());
												
												
											} catch (Exception e) {
												synchronized (dateStats) {
													dateStats.remove(currentDateTime);

												}
											}
											
											synchronized (dateStats) {
												
												dateStats.remove(currentDateTime);
												
											}
											if (isDebugEnabled()) {

												logger.info("flushStats: removing from cache id: " + stat.getId() + " " + stat.getTs() + " " + stat.getStatGranularity() + " " + stat.getStatClass() + " " + stat.getEntityId());
												logger.info("flushStats: " + currentDateTime);
											}
										} else if (isDebugEnabled()){
											try {
												
												Object statLockObj = this.getLockObj(stat.getLockObjId());
												synchronized (statLockObj) {
													stat = treatStatFlush(stat);
													
												}
												
												synchronized (dateStats) {
													dateStats.put(currentDateTime, stat);

												}
												
											} catch (Exception e) {
												logger.warn("flushStats: " + e);	
												dateStats.remove(currentDateTime);

												
											}
										}
									}
									
								//}
								
								
							}
						}
					}
				}
			}
		}
		
		if (isDebugEnabled()) {
			Long now2 = System.currentTimeMillis();
			logger.info("flushStats: ended @" + now2);
			logger.info("flushStats: total " + (now2 - now) + "ms");
			
			

		}
	}
	
	
	public void flushStats(String statClass, String entityId)  {
		
			Map<String, Map<StatGranularity, Map<Instant, Stat>>> scGanularityStats = stats.get(statClass);

			if (scGanularityStats != null) {
				
					Map<StatGranularity, Map<Instant, Stat>> granularityStats = scGanularityStats.get(entityId);
					if (granularityStats != null) {
						
						for (Iterator<StatGranularity> j = granularityStats.keySet().iterator(); j.hasNext();) {
							StatGranularity statGranularity = j.next();
							Map<Instant, Stat> dateStats = granularityStats.get(statGranularity);
							if (dateStats != null) {
																//synchronized (dateStats) {
									Set<Instant> dateStatsKeySet = dateStats.keySet();
									
									for (Iterator<Instant> k = dateStatsKeySet.iterator(); k.hasNext();) {
										Instant currentDateTime = null;
										synchronized (dateStats) {
											currentDateTime = k.next();
										}
										 
										Stat stat = null;
										
										synchronized (dateStats) {
											stat = dateStats.get(currentDateTime);
											stat = treatStatFlush(stat);
											dateStats.put(currentDateTime, stat);
										}
										
										
									}
									
								
								
							}
						}
					
				}
			
		}
		
		
	}
	
	
	private Instant getHourTs(Instant ts) {
		
		ZonedDateTime zonedInstant = ZonedDateTime
				.ofInstant(ts,tz);
		zonedInstant = zonedInstant.truncatedTo(ChronoUnit.HOURS);
		return zonedInstant.toInstant();
		
	}
	
	private Instant getDayTs(Instant ts) {
		
		ZonedDateTime zonedInstant = ZonedDateTime
				.ofInstant(ts,tz);
		zonedInstant = zonedInstant.truncatedTo(ChronoUnit.DAYS);
		return zonedInstant.toInstant();
		
	}

	private Instant getMonthTs(Instant ts) {
		
		ZonedDateTime zonedInstant = ZonedDateTime
				.ofInstant(ts,tz);
		zonedInstant = zonedInstant.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
		return zonedInstant.toInstant();
		
		
	}

	private Duration getDurationOffsetForGranularity(StatGranularity statGranularity) {
		Integer maxUnits = inMemoryPastUnits;
		switch (statGranularity) {
		case HOUR: { return Duration.of(-maxUnits, ChronoUnit.HOURS); }
		case DAY: { return Duration.of(-maxUnits, ChronoUnit.DAYS); }
		case MONTH: { return Duration.of(-maxUnits*30, ChronoUnit.DAYS); }
		
		}
		return null;
	}

	
	
	
	public void notifyStatFlushLockObj() {
		Object lockObj = getLockObj(this.getClass().getSimpleName() + "statFlush");
		if (lockObj != null) {
			synchronized(lockObj) {
				lockObj.notifyAll();
			}
		}
	}

	
	public void setStartedService(boolean started) {
		startedService = started;
		logger.info("setStartedService: " + started);
	}

	
	/*public StatVO getSumLastNStatVO(String statClass, 
			Long entityId, 
			StatGranularity statGranularity, 
			Instant currentDateTime, int n) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException  {
		StatVO vo = null;
		
		
		ZonedDateTime currentZdt = ZonedDateTime.ofInstant(currentDateTime, tz);
		
		List<Stat> toSum = null;
		//logger.info("QQ: n: " + n + " " + inMemoryPastUnits);
		if (n>0 && (n<=inMemoryPastUnits)) {
			//logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES");
			int idx = n;
			toSum = new ArrayList<Stat>(n);
			while (true) {
			//	logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES get stat");
				Stat stat = this.getStat(statClass, entityId, statGranularity, currentZdt.toInstant(), true);
			//	logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES stat: " + stat);
				toSum.add(stat);
				idx--;
				if (idx<1) break;
				currentZdt = getZTSBefore(currentZdt, statGranularity);
				
			}
			
		}
		//logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES toSum: " + toSum);
		if (toSum != null) {
			vo = sumStats(statClass, entityId, statGranularity, toSum);
		} else {
			vo = new StatVO();
		}
		
		return vo;
	}
*/

	

	
	private static Map<String, Object> lockObjs = new ConcurrentHashMap<String, Object>();
	
	protected Object getLockObj(String identifier) {
		
		Object lockObj = null;
		
		synchronized(lockObjs) {
			lockObj = lockObjs.get(identifier);
			if (lockObj == null) {
				lockObj = new Object();
				lockObjs.put(identifier, lockObj);
			}
		}
		
		
		return lockObj;
	}
	
	protected Object removeLockObj(String identifier) {
		Object object = null;
		synchronized (lockObjs) {
			object = lockObjs.remove(identifier);
		}
		return object;
		
	}
	
	protected int getLockObjsSize() {
		int size = 0;
		synchronized (lockObjs) {
			size = lockObjs.size();
		}
		return size;
	}

	/*
	 * 
	 * public void stat(String statClass, 
	 * String entityId, 
	 * List<StatEnum> enums, 
	 * Instant ts, int increment) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
	
	
	 */
	public void statEvent(StatEvent stat) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (debug) {
			try {
				logger.info("statEvent: stating " + JsonUtil.serialize(stat, false));
			} catch (JsonProcessingException e) {
				
			}
		}
		
		this.stat(stat.getStatClass(), stat.getEntityId(), stat.getEnums(), stat.getTs(), stat.getIncrement(), stat.getDoubleIncrement());

		
		
				
				
	}








	public ZoneId getTz() {
		return tz;
	}








	public void setTz(ZoneId tz) {
		this.tz = tz;
	}

}
