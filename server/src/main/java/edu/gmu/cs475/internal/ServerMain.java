package edu.gmu.cs475.internal;

import edu.gmu.cs475.IKeyValueServer;
import edu.gmu.cs475.KeyValueServer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class ServerMain {

	public static Path BASEDIR = Paths.get(System.getProperty("user.dir").replace("client","server"), "testdir");;
	public static void main(String[] args) throws IOException {
		if(args.length != 1)
		{
			System.err.println("Error: expected usage: java -jar server.jar <portnumber>");
			return;
		}
		int port = Integer.valueOf(args[0]);
		KeyValueServer keyValueServer = new KeyValueServer();
		IKeyValueServer stub = (IKeyValueServer) UnicastRemoteObject.exportObject(keyValueServer, 0);
		Registry registry = LocateRegistry.createRegistry(port);
		registry.rebind(IKeyValueServer.RMI_NAME, stub);
		System.out.println("Server bound to port " + port);
		
	}

}
