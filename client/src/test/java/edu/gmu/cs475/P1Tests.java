package edu.gmu.cs475;

import edu.gmu.cs475.internal.CaptureMatcher;
import edu.gmu.cs475.internal.NoopKeyValueClient;
import edu.gmu.cs475.internal.ServerMain;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.internal.ThrowableWrapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;

/**
 * Created by jon on 3/19/18.
 */
public class P1Tests {

	private static final int N_FILES = 20;
	private static final int N_REPLICAS = 20;
	@Rule
	public Timeout globalTimeout = new Timeout(22000);
	boolean err;

	@Test
	public void testClientInitThenRead() throws Exception {
		IKeyValueServer server = mock(IKeyValueServer.class);
		Capture<String> registeredHostName = Capture.newInstance();
		Capture<Integer> registeredPort = Capture.newInstance();
		expect(server.registerClient(capture(registeredHostName), captureInt(registeredPort))).andAnswer(new IAnswer<HashMap<String, String>>() {

			@Override
			public HashMap<String, String> answer() throws Throwable {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("file" + i, "Contents " + i);
				return ret;
			}
		}).once();
		server.cacheDisconnect(CaptureMatcher.matchesCaptured(registeredHostName), CaptureMatcher.matchesCapturedInt(registeredPort));
		expectLastCall().once();
		replay(server);
		AbstractKeyValueClient fileManager = new KeyValueClient(server);
		try {
			for (int i = 0; i < N_FILES; i++)
				assertEquals("Contents " + i, fileManager.get("file" + i));
		} finally {
			fileManager.cleanup();
			verify(server);
		}
	}

	@Test
	public void testClientInitThenCacheUpdateThenRead() throws Exception {
		IKeyValueServer server = mock(IKeyValueServer.class);
		Capture<String> registeredHostName = Capture.newInstance();
		Capture<Integer> registeredPort = Capture.newInstance();
		expect(server.registerClient(capture(registeredHostName), captureInt(registeredPort))).andAnswer(new IAnswer<HashMap<String, String>>() {

			@Override
			public HashMap<String, String> answer() throws Throwable {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("file" + i, "Contents " + i);
				return ret;
			}
		}).once();
		server.cacheDisconnect(CaptureMatcher.matchesCaptured(registeredHostName), CaptureMatcher.matchesCapturedInt(registeredPort));
		expectLastCall().once();
		replay(server);
		AbstractKeyValueClient fileManager = new KeyValueClient(server);
		try {
			for (int i = 0; i < N_FILES; i++)
				assertEquals("Contents " + i, fileManager.get("file" + i));
			for (int i = 0; i < N_FILES; i++)
				fileManager.innerWriteKey("file" + i, "updated contents " + i, 0);
			for (int i = 0; i < N_FILES; i++)
				assertEquals("updated contents " + i, fileManager.get("file" + i));
		} finally {
			fileManager.cleanup();
			verify(server);
		}
	}

	static ArrayList<String> populateServer(KeyValueServer server)
	{
		ArrayList<String> ret = new ArrayList<>();
		for(int i = 0; i < 5; i++)
		{
			for(int j = 0; j < 5; j++){
				server._set("/key"+j+"/path"+i, "hello");
				ret.add("/key"+j+"/path"+i);
			}
		}
		return ret;
	}
	@Test
	public void testServerManyClientsRegisterAllGetInnerWrite() throws Exception {
		//Get a real server
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		//Set up fake clients
		KeyValueClient[] clients = new KeyValueClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(KeyValueClient.class);
			for (String p : files)
				expect(clients[i].innerWriteKey(eq(p.toString()), eq(contentToWrite + p.toString()), anyLong())).andReturn(true);
			clients[i].commitTransaction(anyLong());
			expectLastCall().anyTimes();
			clients[i].abortTransaction(anyLong());
			expectLastCall().anyTimes();

			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerClient("fake hostname", 9000 + i, clients[i]);
		}
		for (String p : files)
			server.set(p.toString(), contentToWrite + p.toString());
		for (int i = 0; i < N_REPLICAS; i++) {
			server.cacheDisconnect("fake hostname", 9000 + i);
		}
		for (KeyValueClient client : clients)
			verify(client);

		//Last, check that the server has all of the right files.
		KeyValueClient fake = mock(KeyValueClient.class);
		HashMap<String, String> endFiles = server.registerClient("fake", 9, fake);
		for (Map.Entry<String, String> e : endFiles.entrySet()) {
			String expected = contentToWrite + e.getKey();
			if (!e.getValue().equals(expected))
				fail("Writes were not saved on the server, expected file content " + expected + " but got " + e.getValue());
		}
	}

	@Test
	public void testServersetPropogatesErrors() throws Exception {
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		//Set up fake clients
		KeyValueClient[] clients = new KeyValueClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(KeyValueClient.class);
			if (i == N_REPLICAS / 2)
				expect(clients[i].innerWriteKey(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(false).times(1);
			else
				expect(clients[i].innerWriteKey(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(true).times(0, 1);
			clients[i].commitTransaction(anyLong());
			expectLastCall().anyTimes();
			clients[i].abortTransaction(anyLong());
			expectLastCall().anyTimes();

			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerClient("fake hostname", 9000 + i, clients[i]);
		}
		boolean caught = false;
		try {
			server.set(files.get(0).toString(), contentToWrite);
		} catch (IOException ex) {
			caught = true;
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.cacheDisconnect("fake hostname", 9000 + i);
		}
		if (!caught)
			fail("Expected set to throw an IOException if a single write failed");
		for (KeyValueClient client : clients)
			verify(client);
	}

	@Test
	public void testServerDisconnectedClientsDontGetWrite() throws Exception {
		//Get a real server
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		//Set up fake clients
		KeyValueClient[] clients = new KeyValueClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(KeyValueClient.class);

			if (i % 2 == 0) {
				//Half of the clients will expect to get the write
				for (String p : files)
					expect(clients[i].innerWriteKey(eq(p.toString()), eq(contentToWrite + p.toString()), anyLong())).andReturn(true);
				clients[i].commitTransaction(anyLong());
				expectLastCall().anyTimes();
				clients[i].abortTransaction(anyLong());
				expectLastCall().anyTimes();
			} else {
				//Other half of the clients will expect to get nothing!
			}

			replay(clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			server.registerClient("fake hostname", 9000 + i, clients[i]);
		}
		for (int i = 0; i < N_REPLICAS; i++) {
			if (i % 2 != 0) {
				server.cacheDisconnect("fake hostname", 9000 + i);
			}
		}
		for (String p : files)
			server.set(p.toString(), contentToWrite + p.toString());
		for (int i = 0; i < N_REPLICAS; i++) {
			if (i % 2 == 0)
				server.cacheDisconnect("fake hostname", 9000 + i);
		}
		for (KeyValueClient client : clients)
			verify(client);
	}

	@Test
	public void testServerMutualExclusionOnWrite() throws Exception {
		final KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		final long TIMEOUT = 500;
		this.err = false;
		//We can make a fake client that blocks in its write then make sure that another client can't get through at the same time
		AtomicInteger nConcurrentWrites = new AtomicInteger();
		AbstractKeyValueClient slowWriter = new NoopKeyValueClient(server) {
			@Override
			public void commitTransaction(long id) {
			}

			@Override
			public void abortTransaction(long id) throws RemoteException {
			}

			@Override
			public boolean innerWriteKey(String file, String content, long xid) {
				if (nConcurrentWrites.incrementAndGet() > 1) {
					System.err.println("\nError: Detected concurrent writes to " + file);
					err = true;
					return true;
				}
				try {
					Thread.sleep(TIMEOUT);
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
				nConcurrentWrites.decrementAndGet();
				return true;
			}
		};


		server.registerClient("fake client", 90040, slowWriter);
		Thread writerThread = new Thread(() -> {
			try {
				server.set(files.get(0).toString(), "garbage");
			} catch (IOException e) {
				e.printStackTrace();
				err = true;
			}
		});

		Thread writer2Thread = new Thread(() -> {
			try {
				server.set(files.get(0).toString(), "garbage2");
			} catch (IOException e) {
				e.printStackTrace();
				err = true;
			}
		});
		writerThread.start();
		writer2Thread.start();
		writerThread.join();
		writer2Thread.join();
		server.cacheDisconnect("fake client", 90040);
		assertFalse("See error above", err);
	}

	@Test
	public void testServerMutualExclusionOnWriteDoesntApplyDifferentFiles() throws Exception {
		final KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		final long TIMEOUT = 100;
		this.err = true;
		//We can make a fake client that blocks in its write then make sure that another client can't get through at the same time
		// KeyValueClient slowWriter = mock(KeyValueClient.class);
		AtomicInteger nConcurrentWrites = new AtomicInteger();
		AbstractKeyValueClient slowWriter = new NoopKeyValueClient(server) {
			@Override
			public void commitTransaction(long id) {
			}

			@Override
			public void abortTransaction(long id) throws RemoteException {
			}

			@Override
			public boolean innerWriteKey(String file, String content, long xid) {
				if (nConcurrentWrites.incrementAndGet() > 1) {
					err = false;
					return true;
				}
				try {
					Thread.sleep(TIMEOUT);
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
				nConcurrentWrites.decrementAndGet();
				return true;
			}
		};

		server.registerClient("fake client", 90040, slowWriter);
		Thread writerThread = new Thread(() -> {
			try {
				server.set(files.get(0).toString(), "garbage");
			} catch (Throwable e) {
				e.printStackTrace();
				err = true;
			}
		});

		Thread writer2Thread = new Thread(() -> {
			try {
				server.set(files.get(1).toString(), "garbage2");
			} catch (Throwable e) {
				e.printStackTrace();
				err = true;
			}
		});
		writerThread.start();
		writer2Thread.start();
		writerThread.join();
		writer2Thread.join();
		server.cacheDisconnect("fake client", 90040);
//        verify(slowWriter);
		assertFalse("Expected that two threads writing different files could write concurrently", err);
	}

	@Test
	public void testServerReplicaLeavingDuringWrite() throws Exception {
		//Start with 2 replicas. 1 tries to leave immediately before it writes. Should not be able to leave.
		final KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		final long TIMEOUT = 100;
		this.err = false;
		//We can make a fake client that blocks in its write then make sure that another client can't get through at the same time
		// KeyValueClient slowWriter = mock(KeyValueClient.class);

		AtomicInteger startedWrites = new AtomicInteger();
		AtomicInteger completedWrites = new AtomicInteger();
		FirstGuyLeavesWhileWritingClient slowWriter1 = new FirstGuyLeavesWhileWritingClient(server, startedWrites, 90490, completedWrites);
		FirstGuyLeavesWhileWritingClient slowWriter2 = new FirstGuyLeavesWhileWritingClient(server, startedWrites, 90492, completedWrites);
		server.registerClient("fake client", 90040, slowWriter1);
		server.registerClient("fake client", 90041, slowWriter2);
		Thread writerThread = new Thread(new Runnable() {
			public void run() {
				try {
					server.set(files.get(0).toString(), "garbage");
			} catch (Throwable e) {
					e.printStackTrace();
					err = true;
				}
		}});
		writerThread.start();
		writerThread.join();
		server.cacheDisconnect("fake client", 90040);
		server.cacheDisconnect("fake client", 90041);
		assertFalse("Expected that client could not disconnect until after pending write completed", err);
	}

	class FirstGuyLeavesWhileWritingClient extends NoopKeyValueClient {
		AtomicInteger completedWrites;
		AbstractKeyValueServer server;
		int port;
		AtomicInteger startedWrites;

		FirstGuyLeavesWhileWritingClient(AbstractKeyValueServer server, AtomicInteger startedWrites, int port, AtomicInteger completedWrites) {
			super(server);
			this.completedWrites = completedWrites;
			this.server = server;
			this.port = port;
			this.startedWrites = startedWrites;
		}

		@Override
		public void commitTransaction(long id) {
		}

		@Override
		public void abortTransaction(long id) throws RemoteException {
		}

		@Override
		public boolean innerWriteKey(String file, String content, long xid) {
			if (startedWrites.getAndIncrement() == 1) {
				//We are the guy who should leave
				Thread t = new Thread(() -> {
					try {
						//Concurrency: this method can't return until the writes are all completed
						server.cacheDisconnect("foo", port);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
				});
				t.start();
				try {
					t.join(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(!t.isAlive()){
					err = true;
					System.err.println("Error: cacheDisconnect was allowed to return (and/or threw an exception) while a write was in progress!");
				}
			}
			try {
				Thread.sleep(400);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		}
	}


}
