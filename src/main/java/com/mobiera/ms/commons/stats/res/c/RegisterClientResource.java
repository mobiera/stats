package com.mobiera.ms.commons.stats.res.c;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import com.mobiera.commons.api.RegisterRequest;
import com.mobiera.commons.api.RegisterResponse;
import com.mobiera.commons.exception.ClientException;


@RegisterProvider(value = ClientResponseExceptionMapper.class)
@RegisterRestClient
public interface RegisterClientResource {


	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("/register")
	RegisterResponse registerRequest(RegisterRequest re) throws ClientException;
	
}
