package edu.gmu.cs475;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Set;

public interface IKeyValueServer extends Remote {
	public static final String RMI_NAME = "cs475IKeyValueStore";

	String get(String key) throws RemoteException;

	Set<String> listKeys() throws RemoteException;

	void set(String key, String value) throws RemoteException, IOException;

	Set<String> listDirectory(String directory) throws RemoteException;

	long lockKey(String name) throws RemoteException;

	void unLockKey(String name, long stamp) throws RemoteException;

	boolean setInTransaction(String key, String value, long xid) throws RemoteException;

	public HashMap<String, String> registerClient(String hostname, int portNumber) throws RemoteException;

	/**
	 * Notifies the server that a cache client is shutting down (and hence no longer will be involved in writes)
	 *
	 * @param hostname   The hostname of the client that is disconnecting (same hostname specified when it registered)
	 * @param portNumber The port number of the client that is disconnecting (same port number specified when it registered)
	 * @throws RemoteException
	 */
	public void cacheDisconnect(String hostname, int portNumber) throws RemoteException;

	/**
	 * Request a new transaction ID to represent a new, client-managed transaction
	 *
	 * @return Transaction organizer-provided ID that will be used in the future to commit or abort this transaction
	 */
	public long startNewTransaction() throws RemoteException;

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be committed
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 */
	public void issueCommitTransaction(long xid) throws RemoteException;

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be aborted
	 * You must not allow a client to register or depart during an abort.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 */
	public void issueAbortTransaction(long xid) throws RemoteException;
}
