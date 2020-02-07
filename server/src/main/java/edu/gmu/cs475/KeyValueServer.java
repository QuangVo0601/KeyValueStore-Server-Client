package edu.gmu.cs475;


import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class KeyValueServer extends AbstractKeyValueServer {
	
	// a map to keep track of clients & port number that have the replicas
	private Map<Integer, IKeyValueReplica> clients = new HashMap<>();
	
	// a set of updates that were not committed yet
	private Set<ArrayList<String>> notCommittedUpdates = new HashSet<>();
	
	// a map to keep track of stampedLocks
	private Map<String, StampedLock> stampedLocks = new HashMap<>();
	
	private ReentrantReadWriteLock ReadWriteLock = new ReentrantReadWriteLock();
	
	private volatile long transactionID = 1;

	/**
	 * Retrieve an element from this key value store
	 *
	 * @param key the key to retrieve
	 * @return The value mapped to this key, if one exists, otherwise null
	 * @throws NullPointerException if key is null
	 */
	public String get(String key) {
		
		if(key == null) {
			throw new NullPointerException();
		}
		return (String) _get(key);
	}

	/**
	 * Lists all of the keys that are currently known to this key-value store
	 * <p>
	 * * @return A set containing all currently valid keys
	 */
	public Set<String> listKeys() {
		return _listKeys();
	}

	/**
	 * Lists the contents of a directory (non-recursively)
	 *
	 * @param directory path of the directory
	 * @return unsorted set of the files and directories inside of this directory, or null if the directory doesn't exist.
	 * @throws NullPointerException if key is null
	 */
	public Set<String> listDirectory(String directory) {
		
		if(directory == null) {
			throw new NullPointerException();
		}
		Set<String> keys = listKeys();
		HashSet<String> ret = new HashSet<>();
		for (String s : keys) {
			if (s.startsWith(directory))
				ret.add(s);
		}
		return ret;
	}

	/**
	 * Sets a key to be the given value
	 *
	 * Creates a transaction (for part 1, always uses 0 as transaction id), then locks the key, then calls innerWriteKey on each replica.
	 * If all replicas succeed, commits the transaction and sets the key locally. Otherwise, aborts the transaction.
	 *
	 * @param key   key to set
	 * @param value value to store
	 * @throws NullPointerException if key or value is null
	 * @throws IOException if write fails (also causes transaction to be aborted
	 */
	public void set(String key, String value) throws IOException{
		//TODO transactions, replication!
		if(key == null || value == null) {
			throw new NullPointerException();
		}
		
		ReadWriteLock.readLock().lock(); 
		
		long stamp = lockKey(key);
		
		// new transaction ID for each time that set is called
		long xid = transactionID;
		transactionID++;  
				
		boolean writePass = false;
		
		try {
			//the server tells clients to get ready to do the update
			Set<Integer> keySet = clients.keySet();
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				writePass = replica.innerWriteKey(key, value, xid);
				
				if(writePass == false) { //if one write fails, or one client says no
					throw new IOException();
				}
			}
			
			//If all replicas succeed, commits the transaction and sets the key locally
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				replica.commitTransaction(xid);
			}
			
			_set(key, value);
						
		}
		catch (Exception e) { // if write fails or exception (client votes not to commit), aborts the transaction.
			Set<Integer> keySet = clients.keySet();
			
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				replica.abortTransaction(xid);
			}
			
			throw new IOException(); //throw an IOException if a single write failed
			
		}
		finally {
			unLockKey(key, stamp);
			ReadWriteLock.readLock().unlock();
		}	
	}

	/**
	 * Sets a key to be the given value
	 *
	 * Does NOT create a transaction, but instead just calls innerWriteKey on each replica. No replica can join or depart
	 * during a call to setInTransaction.
	 * If all replicas succeed, returns true, otherwise returns false.
	 *
	 * @param key   key to set
	 * @param value value to store
	 * @throws NullPointerException if key or value is null
	 */
	@Override
	public boolean setInTransaction(String key, String value, long xid) throws RemoteException {
		//TODO
		if(key == null || value == null) {
			throw new NullPointerException();
		}
		
		ReadWriteLock.readLock().lock();
		
		boolean writePass = false;
		
		try {
			//reach all of the clients and tell them that they should get ready to do the update
			Set<Integer> keySet = clients.keySet();
			
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				writePass = replica.innerWriteKey(key, value, xid);
				
				ArrayList<String> keyValuePair = new ArrayList<String>();
				keyValuePair.add(key);
				keyValuePair.add(value);
				notCommittedUpdates.add(keyValuePair);
				
				//if one write fails, or one client says no
				if(writePass == false) { 
					break;
				}
			}
		}
		finally {
			ReadWriteLock.readLock().unlock();
		}
		
		return writePass;
	}

	/**
	 * Locks (for writing) the given key and returns the stamp
	 *
	 * @param name key
	 * @return stamp for lock
	 */
	@Override
	public long lockKey(String name) {
		//TODO
		
		long stamp = acquireLock(name).writeLock();
		
		return stamp;
	}

	/**
	 * Unlocks the specified key, given the specified stamp
	 *
	 * @param name
	 * @param stamp
	 */
	@Override
	public void unLockKey(String name, long stamp) {
		//TODO
		
		acquireLock(name).unlockWrite(stamp);
	}


	/**
	 * Registers that a client is joining the server. Performs whatever bookkeeping is necessary, and returns a copy
	 * of the current key/values set
	 *
	 * @param hostname   the hostname of the replica talking to you (passed again at disconnect)
	 * @param portNumber the port number of the replica talking to you (passed again at disconnect)
	 * @param replica    The RMI object to use to signal to the replica
	 * @return
	 */
	@Override
	public HashMap<String, String> registerClient(String hostname, int portNumber, IKeyValueReplica replica) {
		//TODO - ensure no clients are able to receive a copy of the underlying map until any pending commit/abort's are finished
		
		ReadWriteLock.writeLock().lock();
		try {
			
			clients.put(portNumber, replica); //registers that a client is joining the server
			
			return copyUnderlyingMap(); //returns a copy of the current key/values set
		}
		finally {
			ReadWriteLock.writeLock().unlock();
		}
		
	}

	/**
	 * Request a new transaction ID to represent a new, client-managed transaction
	 *
	 * @return Server-provided ID that will be used in the future to commit or abort this transaction
	 */
	@Override
	public long startNewTransaction() {
		//TODO
		
		long xid = transactionID;
		transactionID++; //a new transaction ID to represent a new, client-managed transaction
		
		return xid;
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be committed
	 *
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 * @throws RemoteException if any RemoteException occurs in the process of committing
	 */
	@Override
	public void issueCommitTransaction(long xid) throws RemoteException {
		//TODO
		ReadWriteLock.readLock().lock();
		try {
			//Broadcast to all replicas that a transaction should be committed
			Set<Integer> keySet = clients.keySet();
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				replica.commitTransaction(xid);
			}
			
			for(ArrayList<String> keyValuePair : notCommittedUpdates){
				String key = keyValuePair.get(0);
				String value = keyValuePair.get(1);
				
				_set(key, value); //make updates locally as necessary on the server
			}
			
			notCommittedUpdates.clear(); //clear the updates after committing

		}
		finally {
			ReadWriteLock.readLock().unlock();
		}
		
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be aborted
	 *
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 * @throws RemoteException if any RemoteException occurs in the process of committing
	 */
	@Override
	public void issueAbortTransaction(long xid) throws RemoteException {
		//TODO
		
		ReadWriteLock.readLock().lock();
		try {
			//Broadcast to all replicas that a transaction should be aborted
			Set<Integer> keySet = clients.keySet();
			for(int i : keySet) {
				IKeyValueReplica replica = clients.get(i);
				replica.abortTransaction(xid);
			}
			
			notCommittedUpdates.clear(); //clear the updates after the abortion

		}
		finally {
			ReadWriteLock.readLock().unlock();
		}
	}


	/**
	 * Notifies the server that a cache client is shutting down (and hence no longer will be involved in writes)
	 *
	 * @param hostname   The hostname of the client that is disconnecting (same hostname specified when it registered)
	 * @param portNumber The port number of the client that is disconnecting (same port number specified when it registered)
	 */
	@Override
	public void cacheDisconnect(String hostname, int portNumber) {
		//TODO - ensure no clients are able to complete this method until any pending commits/abort's are finished
		
		ReadWriteLock.writeLock().lock();
		try {
			clients.remove(portNumber); //disconnecting a client with the specified port number
		}
		finally {
			ReadWriteLock.writeLock().unlock();
		}
	}
	
	/**
	 * helper method to acquire a stampedLock
	 * @param key
	 * @return
	 */
	synchronized protected StampedLock acquireLock(String key){

		if(stampedLocks.containsKey(key) == false) {

			StampedLock lock = new StampedLock();
			stampedLocks.put(key, lock);

			return lock;

		}
		else{

			return stampedLocks.get(key);

		}
	}
}
