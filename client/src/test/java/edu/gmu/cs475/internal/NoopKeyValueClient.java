package edu.gmu.cs475.internal;

import edu.gmu.cs475.AbstractKeyValueClient;
import edu.gmu.cs475.IKeyValueServer;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Set;

public class NoopKeyValueClient extends AbstractKeyValueClient {
	protected NoopKeyValueClient(String host, int port) {
		super(host, port);
	}

	protected NoopKeyValueClient(IKeyValueServer server) {
		super(server);
	}

	@Override
	protected void initReplica(HashMap<String, String> keysAndValues) {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public String get(String key) {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public Set<String> listDirectory(String directory) {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public Set<String> listKeys() {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public void putAll(String directory, String content) throws RemoteException {
		throw new AssertionError("Unexpected method call");

	}

	@Override
	public boolean innerWriteKey(String key, String content, long xid) throws RemoteException {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public void commitTransaction(long id) throws RemoteException {
		throw new AssertionError("Unexpected method call");
	}

	@Override
	public void abortTransaction(long id) throws RemoteException {
		throw new AssertionError("Unexpected method call");
	}
}
