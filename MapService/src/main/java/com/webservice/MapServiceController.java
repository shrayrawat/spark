package com.webservice;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.kafka.Manager;
import com.models.LocationHolder;

import io.dropwizard.jersey.PATCH;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MapServiceController {

	@GET
	@Path("topic/{topicName}/next")
	public LocationHolder getNextGeoPoint(@PathParam("topicName") String topicName,
			@QueryParam("clientId") String clientId) throws InterruptedException {
		LocationHolder loc = null;
		System.out.println("topicName = " + topicName);
		System.out.println("clientId = " + clientId);
		try {
			loc = Manager.get().getNextMessage(topicName, clientId);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("loc = " + loc);
		if (loc == null)
			throw new NotFoundException("No data found.");
		return loc;
	}

	/**
	 * API to enable/disable a rule
	 *
	 * @param topicName
	 * @return
	 */
	@PATCH
	@Path("topic/{topicName}/reset")
	public Response resetTopicConsumer(@PathParam("topicName") String topicName,
			@QueryParam("clientId") String clientId) throws IOException, InterruptedException {
		Manager.get().reset(topicName, clientId);
		return Response.ok("Topic reset.").build();
	}

	/**
	 * Health API
	 *
	 * @return
	 */
	@GET
	@Path("health")
	public Response getScore() {
		return Response.ok("OK: Service is Up !").build();
	}

}
