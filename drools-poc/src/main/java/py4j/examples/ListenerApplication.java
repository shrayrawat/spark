package py4j.examples;

import py4j.GatewayServer;

public class ListenerApplication {

	ExampleWithField field;

	public void registerObj(ExampleWithField listener) {
		field = listener;
	}

	public void notifyAllListeners() {
		System.out.println(field.member);
		System.out.println(field.name);
	}

	public static void main(String[] args) {
		ListenerApplication application = new ListenerApplication();
		GatewayServer server = new GatewayServer(application);
		server.start(true);
	}
}