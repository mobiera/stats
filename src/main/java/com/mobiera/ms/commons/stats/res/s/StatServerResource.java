package com.mobiera.ms.commons.stats.res.s;

import java.time.Instant;
import java.util.ArrayList;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mobiera.commons.util.JsonUtil;
import com.mobiera.ms.commons.stats.api.CompareStatView;
import com.mobiera.ms.commons.stats.api.GetStat;
import com.mobiera.ms.commons.stats.api.GetStatView;
import com.mobiera.ms.commons.stats.api.GetSumLastNStatVO;
import com.mobiera.ms.commons.stats.api.StatGranularity;
import com.mobiera.ms.commons.stats.api.StatVO;
import com.mobiera.ms.commons.stats.api.StatView;
import com.mobiera.ms.commons.stats.svc.StatBuilderService;
import com.mobiera.ms.commons.stats.svc.StatReaderService;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;




@Path("/stats")
public class StatServerResource {

	@Inject
	StatReaderService statReaderService;
	
	@Inject
	StatBuilderService statBuilderService;
	
	@ConfigProperty(name = "com.mobiera.ms.commons.stats.in.memory.past.units")
	Integer inMemoryPastUnits;
	
	
	private static Logger logger = Logger.getLogger(StatServerResource.class);

	@ConfigProperty(name = "com.mobiera.ms.commons.stats.debug")
	Boolean debug;
	
	/*@POST
	@Path("/test")
	@Produces("application/json")
	public Response testMe(GetTestView request) throws JsonProcessingException, InterruptedException {
		GetTestViewResponse resp = new GetTestViewResponse();
		resp.setErrorMessage("uche");
		Response.Status status = Response.Status.OK;
		return Response.status(status).entity(resp).build();
	}
	*/
	
	
	@POST
	@Path("/view")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getStatViewRequest(GetStatView request) throws JsonProcessingException, InterruptedException
	{
		
		
		logger.info("getStatViewRequest: " + request);

		StatView answer = null;
		
		Response.Status status = Response.Status.BAD_REQUEST;

		request.setStatGranularity(this.computeGranularity(request.getFrom(), request.getTo()));

		if ((request.getFrom() == null) ||
				(request.getTo() == null) ||
				(request.getStatClass() == null) ||
				(request.getStatEnums() == null) ||
				(request.getStatGranularity() == null) ||
				(request.getStatResultType() == null) ||
				(request.getStatEnums().size() == 0)
				) {
			//answer.setErrorMessage("missing argument");
			status = Status.BAD_REQUEST;

		} else {

			
			try {
				answer = statReaderService.getStatViewVO(request.getFrom(),
						request.getTo(), request.getEntityFks()==null?new ArrayList<Long>(0):request.getEntityFks(),
								request.getStatClass(), request.getStatGranularity(),
								request.getStatEnums(), request.getStatResultType());
				
				status = Status.OK;
			} 
			catch (Exception e) {
				status = Status.INTERNAL_SERVER_ERROR;
			}
		}
		logger.info("getStatViewRequest: " + JsonUtil.serialize(answer, false));
		
		return Response.status(status).entity(answer).build();

	}
	
	
	@POST
	@Path("/compare")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getCompareStatViewRequest(CompareStatView request) throws JsonProcessingException, InterruptedException
	{
		request.setStatGranularity(this.computeGranularity(request.getFrom(), request.getTo()));
		StatView answer = null;
		Response.Status status = Response.Status.BAD_REQUEST;

		if ((request.getFrom() == null) ||
				(request.getTo() == null) ||
				(request.getKpis() == null) ||
				(request.getStatGranularity() == null) ||
				(request.getStatResultType() == null) ||
				(request.getKpis().size() == 0)
				) {
			status = Status.BAD_REQUEST;
		} else {

			try {
				answer = statReaderService.getCompareStatViewVO(request.getFrom(),
						request.getTo(), request.getKpis(),
						request.getStatGranularity(),
						request.getStatResultType());
				status = Status.OK;

			}
			catch (Exception e) {
				status = Status.INTERNAL_SERVER_ERROR;
				logger.error("", e);
			}
		}

		return Response.status(status).entity(answer).build();

	}
	
	
	
	@POST
	@Path("/sumLastNStatVO")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getSumLastNStatVO(GetSumLastNStatVO request) {
		StatVO answer = null;
		Response.Status status = Response.Status.BAD_REQUEST;

		if (request == null) {
			return Response.status(status).build();
		}
		
		if (request.getStatClass() == null) {
			return Response.status(status).build();
		}
		if (request.getEntityId() == null) {
			return Response.status(status).build();
		}
		if (request.getStatGranularity() == null) {
			return Response.status(status).build();
		}
		if (request.getCurrentDateTime() == null) {
			return Response.status(status).build();
		}
		if (request.getN() == null) {
			return Response.status(status).build();
		}
		
		
		try {
			answer = statReaderService.getSumLastNStatVO(
					request.getStatClass(),
					request.getEntityId(),
					request.getStatGranularity(),
					request.getCurrentDateTime(),
					request.getN()
					);
			status = Response.Status.OK;
			return Response.status(status).entity(answer).build();
		} catch (Exception e) {
			logger.error("", e);
			status = Response.Status.INTERNAL_SERVER_ERROR;
		}
		
		return Response.status(status).build();
	}
	
	

	private StatGranularity computeGranularity(Instant from, Instant to) {
		if ((to.getEpochSecond()-from.getEpochSecond()) < 30l * 3600l) {
			return StatGranularity.HOUR;
		} else if ((to.getEpochSecond()-from.getEpochSecond()) < 70l * 24l * 3600l) {
			return StatGranularity.DAY;
		} else {
			return StatGranularity.MONTH;
		}
	}


	public boolean isDebugEnabled() {
		return (logger.isInfoEnabled() && debug);
	}

	@POST
	@Path("/get")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getStat(GetStat getStat)  {
		if (getStat != null) {
			if (getStat.getEntityFk() != null) {
				if (getStat.getStatClass() != null) {
					if (getStat.getStatGranularity() != null) {
						if (getStat.getTs() != null) {
							StatVO stat = null;
							try {
								stat = statReaderService.getStatVO(getStat.getStatClass(), getStat.getEntityFk(), getStat.getStatGranularity(), getStat.getTs());
							} catch (Exception e) {
								logger.error("", e);
								return Response.status(Status.BAD_REQUEST).entity(stat).build();
							}
							if (stat == null) {
								stat = new StatVO();
							}
							return Response.status(Status.OK).entity(stat).build();
						}
					}
				}
			}
		}
		return Response.status(Status.BAD_REQUEST).build();
	}
		
}