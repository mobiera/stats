package com.mobiera.ms.commons.stats.res.c;

import java.io.ByteArrayInputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.eclipse.microprofile.rest.client.ext.ResponseExceptionMapper;
import org.jboss.logging.Logger;

import com.mobiera.commons.exception.ClientException;


@Provider
public class ClientResponseExceptionMapper implements ResponseExceptionMapper<ClientException> {

	private static final Logger logger = Logger.getLogger(ClientResponseExceptionMapper.class);
	
	@Override
	public ClientException toThrowable(Response response) {
		
		/*java.io.ByteArrayInputStream entity = (ByteArrayInputStream) response.getEntity();
		
		logger.info(new String(entity.readAllBytes()));
		logger.info(response.getStatus());
		*/
		return new ClientException("");
	}
	
	@Override
    public int getPriority() {
        return 0;
    }
}