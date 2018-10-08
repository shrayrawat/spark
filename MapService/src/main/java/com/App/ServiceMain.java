package com.App;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import org.eclipse.jetty.servlets.CrossOriginFilter;

import com.kafka.Manager;
import com.webservice.MapServiceController;
import com.webservice.ServiceConfig;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ServiceMain extends Application<ServiceConfig> {
	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		System.out.println(args[1]);
		new ServiceMain().run(new String[] { args[0], args[1] });
		// new ServiceMain().run("server", "resources/config.yml");
		String cwd = System.getProperty("user.dir");
		System.out.println(cwd);
	}

	@Override
	public void initialize(Bootstrap<ServiceConfig> bootstrap) {
		bootstrap.addBundle(new AssetsBundle("/assets/", "/maps"));
	}

	@Override
	public void run(ServiceConfig conf, Environment env) throws Exception {

		Manager.get().init(conf.getKafkaBroker());
		configureCors(env);
		MapServiceController svcResource = new MapServiceController();
		env.jersey().register(svcResource);
	}

	private void configureCors(Environment environment) {
		final FilterRegistration.Dynamic cors = environment.servlets().addFilter("CORS", CrossOriginFilter.class);

		// Configure CORS parameters
		cors.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
		cors.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM,
				"X-Requested-With,Content-Type,Accept,Origin,Authorization");
		cors.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "OPTIONS,GET,PUT,POST,DELETE,HEAD,PATCH");
		cors.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
		// Add URL mapping
		cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

	}
}