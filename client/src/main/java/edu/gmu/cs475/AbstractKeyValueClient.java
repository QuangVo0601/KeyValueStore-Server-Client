package edu.gmu.cs475;


import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Set;

public abstract class AbstractKeyValueClient implements IKeyValueReplica {

	private IKeyValueServer keyValueServer;

	private int localPort;
	private Registry rmiRegistry;

	protected AbstractKeyValueClient(String host, int port) {
		try {
			Registry registry = LocateRegistry.getRegistry(host, port);
			keyValueServer = (IKeyValueServer) registry.lookup(IKeyValueServer.RMI_NAME);
		} catch (Exception e) {
			System.err.println("Client exception connecting to lock server: " + e.toString());
			e.printStackTrace();
		}
	}

	protected AbstractKeyValueClient(IKeyValueServer server) {
		this.keyValueServer = server;
	}

	protected void startReplica() {
		try {
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				localPort = socket.getLocalPort();
			}
			rmiRegistry = LocateRegistry.createRegistry(localPort);
			IKeyValueReplica replica = (IKeyValueReplica) UnicastRemoteObject.exportObject(this, 0);
			rmiRegistry.rebind(IKeyValueReplica.RMI_NAME, replica);
			System.out.println("Bound replica to " + localPort);
			initReplica(keyValueServer.registerClient("localhost", localPort));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Cleans up the RMI sever that's running the cache client
	 */
	public final void cleanup() {
		try {
			keyValueServer.cacheDisconnect("localhost", localPort);
			UnicastRemoteObject.unexportObject(this, true);
			rmiRegistry.unbind(IKeyValueReplica.RMI_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialzes this read-only replica with the current set of keys and values
	 *
	 * @param keysAndValues A map from keys to values
	 */
	protected abstract void initReplica(HashMap<String, String> keysAndValues);

	public abstract String get(String key);

	public abstract Set<String> listDirectory(String directory);
	public abstract Set<String> listKeys();

	public void set(String key, String value) throws IOException, RemoteException {
		keyValueServer.set(key, value);
	}

	public long lockKey(String name) throws RemoteException {
		long stamp = keyValueServer.lockKey(name);
		return stamp;
	}

	public void unLockKey(String name, long stamp) throws RemoteException {
		keyValueServer.unLockKey(name, stamp);
	}

	public boolean setInTransaction(String key, String value, long xid) throws RemoteException {
		return keyValueServer.setInTransaction(key, value, xid);
	}

	public HashMap<String, String> registerClient(String hostname, int portNumber) throws RemoteException {
		return keyValueServer.registerClient(hostname, portNumber);
	}

	public void cacheDisconnect(String hostname, int portNumber) throws RemoteException {
		keyValueServer.cacheDisconnect(hostname, portNumber);
	}

	public long startNewTransaction() throws RemoteException {
		return keyValueServer.startNewTransaction();
	}

	public void issueCommitTransaction(long xid) throws RemoteException{
		keyValueServer.issueCommitTransaction(xid);
	}

	public void issueAbortTransaction(long xid) throws RemoteException {
		keyValueServer.issueAbortTransaction(xid);
	}

	/**
	 * Sets the content for all keys that are directly contained by the given directory.
	 * Must internally synchronize to guarantee that the list of files in the given directory does
	 * not change during its call, and that each key being set does not
	 * change during its execution (using a read/write lock)
	 * <p>
	 * Given two concurrent calls to putAll, it will be indeterminite
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all keys affected will have the *same* value (and not
	 * some the result of the first, and some the result of the second). Your
	 * code should not deadlock while waiting to acquire locks.
	 * <p>
	 * Your implementation must follow exactly this scheme:
	 * 1. Acquire all locks
	 * 2. Write all values
	 * 3. Release all locks
	 * (You may not release any locks until you are finished writing)
	 * <p>
	 * Must use a transaction to guarantee that all writes succeed to all replicas (or none).
	 *
	 * @param directory Directory to do updates within
	 * @param content   The content to write out to each key
	 * @throws IllegalArgumentException if the key does not represent a directory (does not end in a /)
	 * @throws IllegalArgumentException if the key does not start with a /
	 * @throws RemoteException          if any IOException occurs in the underlying RMI operation
	 */
	public abstract void putAll(String directory, String content) throws RemoteException;

}
