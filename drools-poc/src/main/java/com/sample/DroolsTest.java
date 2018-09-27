package com.sample;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import py4j.GatewayServer;

/**
 * This is a sample class to launch a rule.
 */
public class DroolsTest {

	public static final void main(String[] args) {
		System.out.println("STarting the jar");
		DroolsTest app = new DroolsTest();
		// app is now the gateway.entry_point
		GatewayServer server = new GatewayServer(app);
		server.start();
		/*
		 * try { // load up the knowledge base KieServices ks =
		 * KieServices.Factory.get(); KieContainer kContainer =
		 * ks.getKieClasspathContainer(); KieSession kSession =
		 * kContainer.newKieSession("ksession-rules");
		 * 
		 * // go ! Message message = new Message();
		 * message.setMessage("Hello World"); message.setStatus(Message.HELLO);
		 * kSession.insert(message); kSession.fireAllRules(); } catch (Throwable
		 * t) { t.printStackTrace(); }
		 */
	}

	public void populateMessage(String message2, int status) {
		try {
			// load up the knowledge base
			KieServices ks = KieServices.Factory.get();
			KieContainer kContainer = ks.getKieClasspathContainer();
			KieSession kSession = kContainer.newKieSession("ksession-rules");

			// go !
			Message message = new Message();
			message.setMessage(message2);
			message.setStatus(status);
			kSession.insert(message);
			kSession.fireAllRules();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public static class Message {

		public static final int HELLO = 0;
		public static final int GOODBYE = 1;

		private String message;

		private int status;

		public String getMessage() {
			return this.message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		public int getStatus() {
			return this.status;
		}

		public void setStatus(int status) {
			this.status = status;
		}

	}

}
