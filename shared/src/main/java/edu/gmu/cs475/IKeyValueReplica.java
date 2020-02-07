package edu.gmu.cs475;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IKeyValueReplica extends Remote {
	public static final String RMI_NAME = "cs475IKVStoreReplica";

	/**
	 * Write (or overwrite) a key
	 *
	 * @param key    Key to write
	 * @param content The content desired
	 * @param xid     Transaction ID, if this write is associated with any transaction, or 0 if it is not associated with a transaction
	 *                If it is associated with a transaction, then this write must not be visible until the replicant receives a commit message for the associated transaction ID; if it is aborted, then it is discarded.
	 * @return true if the write was successful and we are voting to commit
	 */
	public boolean innerWriteKey(String key, String content, long xid) throws RemoteException;

	/**
	 * Commit a transaction, making any pending writes immediately visible
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	public void commitTransaction(long id) throws RemoteException;


	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	public void abortTransaction(long id) throws RemoteException;
}
