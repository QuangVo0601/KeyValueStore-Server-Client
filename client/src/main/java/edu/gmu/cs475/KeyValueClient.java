package edu.gmu.cs475;


import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KeyValueClient extends AbstractKeyValueClient {
	
	private ReentrantReadWriteLock ReadWriteLock = new ReentrantReadWriteLock();
	
	private ConcurrentHashMap<String, String> localMap = new ConcurrentHashMap<>();
	
	//a separate cache to store transaction writes that were not committed yet 
	private ConcurrentHashMap<Long, ArrayList<String>> separateCache = new ConcurrentHashMap<>();

	public KeyValueClient(String host, int port) {
		super(host, port);
		startReplica();
	}

	public KeyValueClient(IKeyValueServer server) {
		super(server);
		startReplica();
	}

	/**
	 * Initializes the replica with the current set of keys and their vlaues
	 *
	 * @param keysAndValues A map from keys to values
	 */
	@Override
	protected void initReplica(HashMap<String, String> keysAndValues) {
		localMap.putAll(keysAndValues);
	}

	/**
	 * Returns the value of the given key (or null if the key doesn't exist).
	 *
	 * Does NOT use the server - reads locally from localMap
	 * @param key
	 * @return
	 */
	@Override
	public String get(String key) {
		return localMap.get(key);
	}

	/**
	 * Lists all of the keys
	 *
	 * Does NOT use the server - reads locally from localMap
	 * @return
	 */
	@Override
	public Set<String> listKeys() {
		return localMap.keySet();
	}

	/**
	 * Lists all of the keys that have the given prefix (e.g. are in the same directory)
	 *
	 * Does NOT use the server - reads locally from localMap
	 * @param directory
	 * @return
	 */
	@Override
	public Set<String> listDirectory(String directory) {
		Set<String> keys = localMap.keySet();
		HashSet<String> ret = new HashSet<>();
		for (String s : keys) {
			if (s.startsWith(directory))
				ret.add(s);
		}
		return ret;
	}

	/**
	 * Write a key in our *local* map
	 *
	 * @param key     Key to set
	 * @param content String representing the content desired
	 * @param xid     Transaction ID, if this write is associated with any transaction, or 0 if it is not associated with a transaction
	 *                If it is associated with a transaction, then this write must not be visible until the replicant receives a commit message for the associated transaction ID; if it is aborted, then it is discarded.
	 * @return true always, indicating that your client succeeded in making the update. Your server, however, must not assume that all clients return true (perhaps some other clients will return false if they have some error)
	 * @return true if the write was successful and we are voting to commit
	 */
	@Override
	public boolean innerWriteKey(String key, String content, long xid) {
		//TODO
				
		ReadWriteLock.writeLock().lock();
		try {
			if(xid == 0) { // 0 if it is not associated with a transaction
				localMap.put(key, content);
			}
			
			// stores transaction writes into a separate cache
			ArrayList<String> keyContent = new ArrayList<String>();
			keyContent.add(key);
			keyContent.add(content);
			separateCache.put(xid, keyContent);
						
		}
		finally {
			ReadWriteLock.writeLock().unlock();
		}
		
		return true;
	}

	/**
	 * Commit a transaction, making any pending writes immediately visible.
	 *
	 * If the specified transaction doesn't exist then does nothing
	 *
	 * @param id transaction id
	 */
	@Override
	public void commitTransaction(long id) {
		//TODO
		
		if(separateCache.containsKey(id)) { // if the specified transaction exists
			ArrayList<String> keyContent = separateCache.get(id);
			String key = keyContent.get(0);
			String value = keyContent.get(1);
			localMap.put(key, value); // making any pending writes immediately visible.
		}
		
	}

	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * If the specified transaction doesn't exist then does nothing
	 *
	 * @param id transaction id
	 */
	@Override
	public void abortTransaction(long id) {
		//TODO
		
		if(separateCache.containsKey(id)) { // if the specified transaction exists
			separateCache.remove(id); // discarding any pending writes that are associated with it
		}
	}

	/**
	 * Sets the content for all keys that are directly contained by the given directory.
	 * Must acquire locks on each key such that each key being set can not be modified by another concurrent
	 * putAll.
	 *
	 * Given two concurrent calls to putAll, it will be indeterminite
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all keys affected will have the *same* value (and not
	 * some the result of the first, and some the result of the second). Your
	 * code should not deadlock while waiting to acquire locks.
	 *
	 * Your implementation must follow exactly this scheme:
	 * 1. Acquire all locks
	 * 2. Start a transaction
	 * 3. Write all values
	 * 4. Commit the transaction if all writes succeeded
	 * 5. Release all locks
	 *
	 * @param directory Directory to do updates within
	 * @param content   The content to write out to each key
	 * @throws IllegalArgumentException if the key does not represent a directory (does not end in a /)
	 * @throws IllegalArgumentException if the key does not start with a /
	 * @throws RemoteException          if any RemoteException occurs in the underlying RMI operation
	 */
	public void putAll(String directory, String content) throws RemoteException, IllegalArgumentException {
		//TODO
		
		if(!(directory.charAt(directory.length() - 1) == '/') || !(directory.charAt(0) == '/')) {
			throw new IllegalArgumentException();
		}

		Set<String> contents = listDirectory(directory); //unsorted HaskSet

		//To avoid deadlocks, make sure that in each call to getAll/putAll, you lock keys in the same order. 
		//Note that listDirectory returns an un-ordered set, so you will need to sort it.
		List<String> contentsList = new ArrayList<String>(contents);
		Collections.sort(contentsList);

		List<Long> stampsList = new ArrayList<Long>();
		
		long transactionID = 0;

		long stamp = 0;		
		
		boolean succeed = false;
		
		try {
			//1. Acquire all locks
			for(int i = 0; i < contentsList.size(); i++) {
				String key = contentsList.get(i);
				stamp = lockKey(key); 
				stampsList.add(stamp);
			}
			
			//2. Start a transaction
			//3. Write all values	
			transactionID = startNewTransaction();
			
			for(int i = 0; i < contentsList.size(); i++) {
				String key = contentsList.get(i);
				succeed = setInTransaction(key, content, transactionID); //Update each key by telling the server to, passing that transaction ID
				
				//If one write failed, abort that transaction
				if(succeed == false){
					issueAbortTransaction(transactionID);
					break;
				}
			}
			
			//4. Commit the transaction if all writes succeeded
			if(succeed == true) {
				issueCommitTransaction(transactionID);
			}
			
		}
		catch(RemoteException e){
			issueAbortTransaction(transactionID); //If one throw a RemoteException (vote no), abort that transaction
		}
		finally{
			//5. Release all locks
			for(int i = 0; i < contentsList.size(); i++) {
				String key = contentsList.get(i);
				stamp = stampsList.get(i);
				unLockKey(key, stamp); 
			}
		}	
	}
}

