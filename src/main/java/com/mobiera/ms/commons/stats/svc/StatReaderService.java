	package com.mobiera.ms.commons.stats.svc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Query;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.graalvm.collections.Pair;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.Kpi;
import com.mobiera.ms.commons.stats.api.StatEnum;
import com.mobiera.ms.commons.stats.api.StatGranularity;
import com.mobiera.ms.commons.stats.api.StatResultType;
import com.mobiera.ms.commons.stats.api.StatVO;
import com.mobiera.ms.commons.stats.api.StatView;
import com.mobiera.ms.commons.stats.assembler.StatVOAssembler;
import com.mobiera.ms.commons.stats.model.Stat;

@ApplicationScoped
public class StatReaderService {

	private final static String SUM_START_QUERY = "SELECT ";
	private final static String SUM_END_QUERY_SOME = " FROM Stat s where s.statClass=:statClass AND s.entityId IN :entityIds AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to";
	private final static String SUM_END_QUERY_ALL = " FROM Stat s where s.statClass=:statClass AND s.statGranularity=:statGranularity AND s.ts>=:from AND s.ts<:to";
	private static Logger logger = Logger.getLogger(StatReaderService.class);
	
	
	@Inject
	EntityManager em;
	
	
	
	@Inject
	StatBuilderService statService;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.timezone")
	String timezoneName;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.debug")
	Boolean debug;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.format")
	String dateFormat;
	
	@Inject
	StatVOAssembler statVOAssembler;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.in.memory.past.units")
	Integer inMemoryPastUnits;
	
	private static NumberFormat nf = null;
	
	private NumberFormat getNf() {
		if (nf == null) {
			nf = NumberFormat.getInstance();
			nf.setMaximumIntegerDigits(20);
			nf.setMaximumFractionDigits(2);
		}
		return nf;
	}
	
	private List<String> buildHeader(List<StatEnum> enums) {
		List<String> header = new ArrayList<String>(enums.size());
		for (StatEnum se: enums) {
			header.add(se.getLabel());
		}
		return header;
	}
	
	
	
	public StatView getCompareStatViewVO(Instant from, Instant to, List<Kpi> kpis,
			StatGranularity statGranularity, StatResultType statResultType) throws IllegalAccessException, IllegalArgumentException, 
	InvocationTargetException, NoSuchMethodException, SecurityException {
		
		
		try {
			logger.info("getCompareStatViewVO: kpis: " + JsonUtil.serialize(kpis, false));
		} catch (JsonProcessingException e) {
			logger.error(e);
		}
		statService.notifyStatFlushLockObj();
		
		StatView vo = new StatView();
		vo.setFrom(from);
		vo.setTo(to);
		vo.setStatGranularity(statGranularity);
		
		
		
		ZonedDateTime zLow = ZonedDateTime.ofInstant(from, statService.getTz());
		ZonedDateTime zHigh = ZonedDateTime.ofInstant(from, statService.getTz());
		
		switch (statGranularity) {
		case HOUR: {
			zHigh = zHigh.plus(1, ChronoUnit.HOURS);
			break;
		}
		case DAY: {
			zHigh = zHigh.plus(1, ChronoUnit.DAYS);
			break;
		}
		case MONTH:
		default: {
			zHigh = zHigh.plus(1, ChronoUnit.MONTHS);
			break;
		}
		
		}
		
		List<List<String>> result = new ArrayList<List<String>>(30);
		List<List<Object>> numericResult = new ArrayList<List<Object>>(30);
		
		while (zLow.toInstant().isBefore(to)) {
			
			if (isDebugEnabled()) {
				logger.info("getCompareStatViewVO: zLow: " + zLow + " zHigh: " + zHigh + " to: " + to);
			}
		//while (lowCalendar.getTimeInMillis()<toMillis) {
			
			/*
			 * To build a row, we need to first build column by column
			 */
			List<String> row = new ArrayList<String>(kpis.size() + 1);
			List<Object> rowNum = new ArrayList<Object>(kpis.size() + 1);
			
			List<String> flushed = new ArrayList<String>();
			
			for (Kpi kpi: kpis) {
				
				if ((kpi.getStat() == null) || (kpi.getStat().getIndex() == -1)) {
					row.add(zLow.toInstant().toString());
				} else {
					List<String> entityIds = new ArrayList<String>(1);
					entityIds.add(((Kpi)kpi).getEntityId());
					List<StatEnum> statEnums = new ArrayList<StatEnum>(1);
					
					String statClass = kpi.getEntityClass();
					statEnums.add(kpi.getStat());
					
					if (statClass == null) {
						statClass = kpi.getType().replaceAll("Kpi", "").replaceAll("\\B([A-Z])", "_$1").toUpperCase();
						logger.info("getCompareStatViewVO: generated statClass: " + statClass + " ");
					}
					if (!flushed.contains(kpi.getEntityId())) {
						flushed.add(kpi.getEntityId());
						statService.flushStats(statClass, kpi.getEntityId());
					}
					
						
					Pair<List<String>, List<Object>>  sum = null;
					sum = this.getSumRow(zLow, zHigh, 
							entityIds, statClass, statGranularity, statEnums, statResultType, false);
					List<String> cell = sum.getLeft();
					List<Object> numCell = sum.getRight();
					row.add(cell.iterator().next());
					rowNum.add(numCell.iterator().next());
				}
			
				
				
			}
			if (isDebugEnabled()) {
				logger.info("getCompareStatViewVO: done first step");
			}
			result.add(row);
			numericResult.add(rowNum);
			
			switch (statGranularity) {
			case HOUR: {
				
				zLow = zLow.plus(1, ChronoUnit.HOURS);
				zHigh = zHigh.plus(1, ChronoUnit.HOURS);
				
				break;
			}
			case DAY: {
				zLow = zLow.plus(1, ChronoUnit.DAYS);
				zHigh = zHigh.plus(1, ChronoUnit.DAYS);
				
				
				break;
			}
			case MONTH:
			default: {
				zLow = zLow.plus(1, ChronoUnit.MONTHS);
				zHigh = zHigh.plus(1, ChronoUnit.MONTHS);
				
				break;
			}
			
			}
		}
		
		vo.setStats(result);
		vo.setNumericStats(numericResult);
		

	
	if (statResultType.equals(StatResultType.SUM) || statResultType.equals(StatResultType.LIST_AND_SUM)) {
		
		
		ZonedDateTime zTo = ZonedDateTime.ofInstant(to, statService.getTz());
		ZonedDateTime zFrom = ZonedDateTime.ofInstant(from, statService.getTz());

		List<String> sum = new ArrayList<String>(kpis.size() + 1);
		List<Object> numericSum = new ArrayList<Object>(kpis.size() + 1);
		
		for (Kpi kpi: kpis) {
			
			
			if ((kpi.getStat() == null) || (kpi.getStat().getIndex() == -1)) {
				
				sum.add("");
			} else {
				List<String> entityIds = new ArrayList<String>(1);
				entityIds.add(((Kpi)kpi).getEntityId());
				List<StatEnum> statEnums = new ArrayList<StatEnum>(1);
				statEnums.add(kpi.getStat());
				
				String statClass = kpi.getEntityClass();
				
				if (statClass == null) {
					statClass = kpi.getType().replaceAll("Kpi", "").replaceAll("\\B([A-Z])", "_$1").toUpperCase();
					logger.info("getCompareStatViewVO: generated statClass: " + statClass + " ");
				}
				
				Pair<List<String>, List<Object>>  s = this.getSumRow(zFrom, zTo, 
						entityIds, statClass, statGranularity, statEnums, statResultType, false);
				List<String> cell = s.getLeft();
				List<Object> numCell = s.getRight();
				sum.add(cell.iterator().next());
				
				numericSum.add(numCell.iterator().next());
			}
			
			
			
			
		}
		
		
		
		vo.setSum(sum);
		vo.setNumericSum(numericSum);
	}
	vo.setStatLabels(this.buildCompareHeader(kpis));
	return vo;
		
	}
	
	
	private void logKpi(Kpi kpi) {
		logger.info("kpi: entityId: " + kpi.getEntityId() +  " statClass: " + kpi.getEntityClass() +  " label: " + kpi.getLabel());
	}



	private List<String> buildCompareHeader(List<Kpi> kpis) {
		List<String> header = new ArrayList<String>(kpis.size());
		for (Kpi se: kpis) {
			header.add(se.getLabel());
		}
		return header;
	}



	public StatView getStatViewVO(Instant from, Instant to, List<String> entityIds, String statClass,
			StatGranularity statGranularity, List<StatEnum> statEnums, StatResultType statResultType) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		
		statService.notifyStatFlushLockObj();
		
		if (entityIds != null) {
			for (String id: entityIds) {
				statService.flushStats(statClass, id);
			}
		}
		
		StatView vo = new StatView();
		vo.setFrom(from);
		vo.setTo(to);
		vo.setEntityIds(entityIds);
		vo.setStatClass(statClass);
		vo.setStatGranularity(statGranularity);
		vo.setStatEnums(statEnums);
		
		ZonedDateTime zLow = ZonedDateTime.ofInstant(from, statService.getTz());
		ZonedDateTime zHigh = ZonedDateTime.ofInstant(to, statService.getTz());

		
		switch (statGranularity) {
		case HOUR: {
			zHigh = zHigh.plus(1, ChronoUnit.HOURS);
			break;
		}
		case DAY: {
			zHigh = zHigh.plus(1, ChronoUnit.DAYS);
			break;
		}
		case MONTH:
		default: {
			zHigh = zHigh.plus(1, ChronoUnit.MONTHS);
			break;
		}
		
		}
		
	
			List<List<String>> result = new ArrayList<List<String>>(30);
			
			while (zLow.toInstant().isBefore(to)) {
				Pair<List<String>, List<Object>>  row = this.getSumRow(zLow, zHigh, 
						entityIds, statClass, statGranularity, statEnums, statResultType, true);
				result.add(row.getLeft());
				
				switch (statGranularity) {
				case HOUR: {
					zLow = zLow.plus(1, ChronoUnit.HOURS);
					zHigh = zHigh.plus(1, ChronoUnit.HOURS);
					break;
				}
				case DAY: {
					zLow = zLow.plus(1, ChronoUnit.DAYS);
					zHigh = zHigh.plus(1, ChronoUnit.DAYS);
					break;
				}
				case MONTH:
				default: {
					zLow = zLow.plus(1, ChronoUnit.MONTHS);
					zHigh = zHigh.plus(1, ChronoUnit.MONTHS);
					break;
				}
				
				}
			}
			
			vo.setStats(result);
			
	
		
		if (statResultType.equals(StatResultType.SUM) || statResultType.equals(StatResultType.LIST_AND_SUM)) {
			
			
			ZonedDateTime zTo = ZonedDateTime.ofInstant(to, statService.getTz());
			ZonedDateTime zFrom = ZonedDateTime.ofInstant(from, statService.getTz());

			Pair<List<String>, List<Object>> sum = this.getSumRow(zFrom, zTo, entityIds, statClass, statGranularity, statEnums, statResultType, false);
			vo.setSum(sum.getLeft());
			vo.setNumericSum(sum.getRight());
		}
		vo.setStatLabels(this.buildHeader(statEnums));
		return vo;
	}
	
	



	/*
	 * Get sum for a given Interval, one or more entities
	 */
	private Pair<List<String>, List<Object>> getSumRow(ZonedDateTime from, ZonedDateTime to, List<String> entityIds, String statClass,
			StatGranularity statGranularity, List<StatEnum> statEnums, StatResultType statResultType, boolean setDateLabel) {
		if (isDebugEnabled()) {
			try {
				logger.info("getSumRow: statClass: " + statClass + " statGranularity: " + statGranularity + " from: " + from + " to: " + to + " entityIds: " + entityIds + " statEnums: " + JsonUtil.serialize(statEnums, false));
			} catch (JsonProcessingException e) {
				
			}
		}
		
		if (from.get(ChronoField.HOUR_OF_DAY) != 0) {
			statGranularity = StatGranularity.HOUR;
		} else if (to.get(ChronoField.HOUR_OF_DAY) != 0) {
			statGranularity = StatGranularity.HOUR;
		} else if (from.get(ChronoField.DAY_OF_MONTH) != 1) {
			statGranularity = StatGranularity.DAY;
		} else if (to.get(ChronoField.DAY_OF_MONTH) != 1) {
			statGranularity = StatGranularity.DAY;
		}
		

		String qStr = this.buildQuery(entityIds.size() == 0, statEnums);
		Object[] qResult = null;
		if (isDebugEnabled()) {
			logger.info("getSumRow: query: " + qStr); 
		}
		if (qStr != null) {
			Query q = em.createQuery(qStr)
					.setParameter("statClass", statClass)
					.setParameter("statGranularity", statGranularity)
					.setParameter("from", from.toInstant())
					.setParameter("to", to.toInstant());
			
			if (entityIds.size() > 0) q.setParameter("entityIds", entityIds);
			
			
			List results = q.getResultList();
			//Object[] object = (Object[]) results.get(0);
			
			if (results.size() > 0) {
				Object object = (Object) results.get(0);
				if (object == null) {
					qResult = new Object[1];
					qResult[0] = null;
				} else if (object instanceof Long) {
					qResult = new Object[1];
					qResult[0] = object;
				} else if (object instanceof Double) {
					qResult = new Object[1];
					
					BigDecimal bd = BigDecimal.valueOf((Double)object);
				    bd = bd.setScale(2, RoundingMode.HALF_UP);
				    object = Double.valueOf(bd.doubleValue());
					
					
					
					qResult[0] = object;
				} else {
					qResult = (Object[]) results.get(0);
					//(Object[]) results.stream().findFirst().orElse(null);
				}
				
			} else {
				qResult = new Object[1];
				qResult[0] = null;
			}
		} else {
			qResult = new Object[1];
			qResult[0] = null;
		}
		
		
		
		if (isDebugEnabled()) {
			logger.info("getSumRow resultSize: " + qResult.length);
			for (Object res: qResult) {
				logger.info("getSumRow object: " + res);
				if(res!= null) logger.info("getSumRow string: " + res.toString());
				
			}
		}
		
		
		List<String> result = new ArrayList<String>(statEnums.size());
		List<Object> numericResult = new ArrayList<Object>(statEnums.size());
		
		int i=0;
		for (StatEnum se: statEnums) {
			if (se.getIndex() == -1) {
				if (setDateLabel) {
					/*if (df == null) {
						df = DateTimeFormatter.ofPattern(cacheManager.getAsString(ParameterName.STRING_STAT_TS_FORMAT));
					}*/
					
					
					result.add(from.toInstant().toString());
				} else {
					result.add("");
				}
				
				numericResult.add(0l);
				
			} else {
				if ( (qResult == null) || (qResult[i] == null)) {
					result.add("0");
					numericResult.add(0l);
				} else {
					
					Object qi = qResult[i];
					
					
					
					if (qi instanceof Long) {
						numericResult.add((Long)qi);
						result.add(qi.toString());
					} else if (qi instanceof Double) {
						
						BigDecimal bd = BigDecimal.valueOf((Double)qi);
					    bd = bd.setScale(2, RoundingMode.HALF_UP);
					    numericResult.add(Double.valueOf(bd.doubleValue()));
					    result.add(getNf().format(bd));
					}
					
				}
				i++;
			}
			
		}
		//return result;
		return Pair.create(result, numericResult);

	}
	
	
	public String buildQuery(boolean all, List<StatEnum> enums) {
		StringBuffer queryBuffer = new StringBuffer(SUM_START_QUERY.length() + SUM_END_QUERY_SOME.length() + 3 + enums.size() * 15);
		queryBuffer.append(SUM_START_QUERY);
		boolean first = true;
		
		for (Iterator<StatEnum> i = enums.iterator(); i.hasNext();) {
			StatEnum se = i.next();
			if (se.getIndex()!= -1) {
				
				String type = "long";
				if (se.getIndex()>=128) {
					type = "double";
				}
				
				if (first) {
					queryBuffer
					.append("SUM(s.").append(type);
					first = false;
				} else {
					queryBuffer
					.append(",SUM(s.").append(type);
				}
				
				queryBuffer.append(se.getIndex());
				queryBuffer.append(")");
				
			}
			
		}
		//if (first) return null;
		queryBuffer.append(all?SUM_END_QUERY_ALL:SUM_END_QUERY_SOME);
		String query = queryBuffer.toString();
		if (isDebugEnabled()) {
			logger.info("buildQuery: "+ query);
		}
		return query;
	}
	
	
	public boolean isDebugEnabled() {
		return (logger.isInfoEnabled() && debug);
	}
	
	
	
	public StatVO getStatVO(String statClass, String entityId, StatGranularity statGranularity, Instant currentDateTime) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException  {
		
		Query q = em.createNamedQuery("Stat.get");
		q.setParameter("statClass", statClass);
		q.setParameter("entityId", entityId);
		q.setParameter("statGranularity", statGranularity);
		q.setParameter("ts", currentDateTime);
		
		
		List<Stat> res = (List<Stat>)q.getResultList();
		StatVO vo = sumStats(statClass, entityId, statGranularity, res);
		
		return vo;
		
	}
		
	
	private StatVO sumStats(String statClass, String entityId, StatGranularity statGranularity, List<Stat> toSum) 
			throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		StatVO vo = new StatVO();
		vo.setEntityId(entityId);
		vo.setStatClass(statClass);
		vo.setStatGranularity(statGranularity);
		
		boolean first = true;
		for (Stat s: toSum) {
			
			if (s != null) {
				for (short i=0; i<100; i++) {
					Method getSMethod = Stat.class.getMethod("getLong" + i);
					Method getVMethod = StatVO.class.getMethod("getLong" + i);
					Method setVMethod = StatVO.class.getMethod("setLong" + i, Long.class);
					Long value = (Long) getSMethod.invoke(s);
					if (value != null) {
						if (first) {
							setVMethod.invoke(vo, value);
							first = false;
						} else {
							setVMethod.invoke(vo, 
									((Long)getVMethod.invoke(vo) + value));
						}
					}
					
				}
				for (short i=128; i<137; i++) {
					Method getSMethod = Stat.class.getMethod("getDouble" + i);
					Method getVMethod = StatVO.class.getMethod("getDouble" + i);
					Method setVMethod = StatVO.class.getMethod("setDouble" + i, Double.class);
					Double value = (Double) getSMethod.invoke(s);
					
					if (value != null) {
						BigDecimal bd = BigDecimal.valueOf((Double)value);
					    bd = bd.setScale(2, RoundingMode.HALF_UP);
					    value = Double.valueOf(bd.doubleValue());
						
					    
					    
						if (first) {
							setVMethod.invoke(vo, value);
							first = false;
						} else {
							setVMethod.invoke(vo, 
									((Double)getVMethod.invoke(vo) + value));
						}
					}
					
				}
			}
			
		}
		return vo;
	}


	/*public StatVO getSumLastNStatVO(String statClass, 
			String entityId, 
			StatGranularity statGranularity, 
			Instant currentDateTime, int n) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException  {
		StatVO vo = null;


		ZonedDateTime currentZdt = ZonedDateTime.ofInstant(currentDateTime, statService.getTz());

		List<Stat> toSum = null;
		//logger.info("QQ: n: " + n + " " + inMemoryPastUnits);
		if (n>0 && (n<=inMemoryPastUnits)) {
			//logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES");
			int idx = n;
			toSum = new ArrayList<Stat>(n);
			while (true) {
				//	logger.info("QQ: n: " + n + " " + inMemoryPastUnits + " YES get stat");
				StatVO stat = this.getStatVO(statClass, entityId, statGranularity, currentZdt.toInstant(), true);
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
	}*/


	public StatVO getSumLastNStatVO(String statClass, 
			String entityId, 
			StatGranularity statGranularity, 
			Instant currentDateTime, int n) throws NoSuchMethodException, SecurityException, 
			IllegalAccessException, IllegalArgumentException, InvocationTargetException  {
		StatVO vo = null;


		statService.notifyStatFlushLockObj();
		
		
		ZonedDateTime currentZdt = ZonedDateTime.ofInstant(currentDateTime, statService.getTz());

		
		if (n>0 && (n<=inMemoryPastUnits)) {
			ZonedDateTime toZdt =  getZTSBeforeN(currentZdt, statGranularity, n);
			
			Query q = em.createNamedQuery("Stat.listOne");
			q.setParameter("statClass", statClass);
			q.setParameter("entityId", entityId);
			q.setParameter("statGranularity", statGranularity);
			q.setParameter("from", toZdt.toInstant());
			q.setParameter("to", currentZdt.toInstant());
			
			
			List<Stat> res = (List<Stat>)q.getResultList();
			vo = sumStats(statClass, entityId, statGranularity, res);
			
			
		} else {
			vo = new StatVO();
		}
		
		

		return vo;
	}
	private ZonedDateTime getZTSBefore(ZonedDateTime zts, StatGranularity statGranularity) {
		ZonedDateTime retval = null;
		
		switch (statGranularity) {
		case HOUR: {
			retval =  zts.minus(1, ChronoUnit.HOURS);
			break;
		}
		case DAY: {
			retval =  zts.minus(1, ChronoUnit.DAYS);
			break;
		}
		case MONTH: {
			retval =  zts.minus(1, ChronoUnit.MONTHS);
			break;
		}
		}
		return retval;
	}
	

	private ZonedDateTime getZTSBeforeN(ZonedDateTime zts, StatGranularity statGranularity, int n) {
		ZonedDateTime retval = null;
		
		switch (statGranularity) {
		case HOUR: {
			retval =  zts.minus(n, ChronoUnit.HOURS);
			break;
		}
		case DAY: {
			retval =  zts.minus(n, ChronoUnit.DAYS);
			break;
		}
		case MONTH: {
			retval =  zts.minus(n, ChronoUnit.MONTHS);
			break;
		}
		}
		return retval;
	}

}

