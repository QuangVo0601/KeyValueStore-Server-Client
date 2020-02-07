package edu.gmu.cs475;


import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractKeyValueServer implements IKeyValueServer {
    private final HashMap<String, String> map = new HashMap<>();



    protected HashMap<String, String> copyUnderlyingMap(){
        return new HashMap<>(map);
    }


    /**
     * Registers a replica with the server, returning all of the files that currently exist.
     *
     * @param hostname   the hostname of the replica talking to you (passed again at disconnect)
     * @param portNumber the port number of the replica talking to you (passed again at disconnect)
     * @param replica    The RMI object to use to signal to the replica
     * @return A HashMap of all of the keys that currently exist, mapping from key to its value
     */
    public abstract HashMap<String, String> registerClient(String hostname, int portNumber, IKeyValueReplica replica) throws IOException;

    @Override
    public HashMap<String, String> registerClient(String hostname, int portNumber) throws RemoteException {
        try {
            System.out.println("Looking for replica at " + hostname + " " + portNumber);
            Registry registry = LocateRegistry.getRegistry(hostname, portNumber);
            IKeyValueReplica replica = (IKeyValueReplica) registry.lookup(IKeyValueReplica.RMI_NAME);
            return registerClient(hostname, portNumber, replica);
        } catch (Exception e) {
            System.err.println("Client exception connecting to lead server: " + e.toString());
            e.printStackTrace();
            throw new RemoteException();
        }
    }

    /**
     * Retrieve an item from the underlying store - you must call this from your KeyValueStore
     *
     * @param key key to retrieve
     * @return The value stored at the given key, or null if none exists
     * @throws NullPointerException if key is null
     */
    protected Object _get(String key) {
        if (key == null)
            throw new NullPointerException();
        synchronized (map) {
            return map.get(key);
        }
    }

    /**
     * Add an item to the underlying store - you must call this from your KeyValueStore
     *
     * @param key   key to set
     * @param value value to store
     * @throws NullPointerException if key or value is null
     */
    void _set(String key, String value) {
        if (key == null || value == null) throw new NullPointerException();
        synchronized (map) {
            map.put(key, value);
        }
    }

    /**
     * Remove an item from the underlying store - you must call this from your KeyValueStore
     *
     * @param key key to remove
     * @return true if the value was removed, false if not
     * @throws NullPointerException if key is null
     */
    protected boolean _remove(String key) {
        if (key == null)
            throw new NullPointerException();
        synchronized (map) {
            return map.remove(key) != null;
        }
    }

    /**
     * Enumerates all of the keys currently in the map
     *
     * @return Set containing all currently valid keys
     */
    protected Set<String> _listKeys() {
        synchronized (map) {
            return new HashSet<>(map.keySet());
        }
    }

    @Override
    public String toString() {
        return "KeyValueStore{" +
                "map=" + map +
                '}';
    }

}
