package edu.gmu.cs475;

import edu.gmu.cs475.internal.NoopKeyValueClient;
import edu.gmu.cs475.internal.ServerMain;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static edu.gmu.cs475.P1Tests.populateServer;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Created by jon on 3/19/18.
 */
public class P3Tests {

	private static final int N_FILES = 20;
	private static final int N_REPLICAS = 20;
	@Rule
	public Timeout globalTimeout = new Timeout(22000);
	boolean err;


	@Test
	public void testClientEchoAllLocksThenStartsTransactionThenWritesAndCommits() throws Exception {
		IMocksControl mocker = EasyMock.createControl();
		mocker.resetToStrict();
		IKeyValueServer server = mock(IKeyValueServer.class);
		try {
			expect(server.registerClient(anyString(), anyInt())).andAnswer(() -> {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("/dir/File" + i, "Contents " + i);
				for (int i = 0; i < N_FILES; i++)
					ret.put("/dir2/File" + i, "Contents " + i);
				return ret;
			}).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.lockKey(anyString())).andReturn(40L + i).once();
			expect(server.startNewTransaction()).andReturn(4L).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.setInTransaction(anyString(), eq("foo"), eq(4L))).andReturn(true).once();
			server.issueCommitTransaction(4L);
			expectLastCall().once();
			server.unLockKey(anyString(), anyLong());
			expectLastCall().times(N_FILES);

			server.cacheDisconnect(anyString(), anyInt());
			expectLastCall().once();
			replay(server);

			KeyValueClient client = new KeyValueClient(server);
			try {
				client.putAll("/dir/", "foo");
			} finally {
				client.cleanup();
			}
			verify(server);
		} finally {
			mocker.resetToDefault();
		}
	}

	@Test
	public void testClientLocksThenStartsTransactionThenWritesAndAbortsOnFailure() throws Exception {
		IMocksControl mocker = EasyMock.createControl();
		mocker.resetToStrict();
		IKeyValueServer server = mock(IKeyValueServer.class);
		try {
			expect(server.registerClient(anyString(), anyInt())).andAnswer(() -> {
				HashMap<String, String> ret = new HashMap<String, String>();
				for (int i = 0; i < N_FILES; i++)
					ret.put("/dir/File" + i, "Contents " + i);
				for (int i = 0; i < N_FILES; i++)
					ret.put("/dir2/File" + i, "Contents " + i);
				return ret;
			}).once();
			for (int i = 0; i < N_FILES; i++)
				expect(server.lockKey(anyString())).andReturn(40L + i).once();
			expect(server.startNewTransaction()).andReturn(4L).once();
			for (int i = 0; i < N_FILES - 1; i++)
				expect(server.setInTransaction(anyString(), eq("foo"), eq(4L))).andReturn(true).once();
			expect(server.setInTransaction(anyString(), eq("foo"), eq(4L))).andReturn(false).once();
			server.issueAbortTransaction(4L);
			expectLastCall().once();
			server.unLockKey(anyString(), anyLong());
			expectLastCall().times(N_FILES);

			server.cacheDisconnect(anyString(), anyInt());
			expectLastCall().once();
			replay(server);

			KeyValueClient client = new KeyValueClient(server);
			try {
				client.putAll("/dir/","foo");
			} finally {
				client.cleanup();
			}
			verify(server);
		} finally {
			mocker.resetToDefault();
		}
	}

	@Test
	public void testServersetInTransactionDoesNotLock() throws Exception {
		KeyValueServer server = new KeyValueServer() {
			@Override
			public long lockKey(String name) {
				fail("lock file should not be called by setInTransaction");
				return 0;
			}

			@Override
			public void unLockKey(String name, long stamp) {
				fail("lock file should not be called by setInTransaction");
			}
		};
		KeyValueClient client = mock(KeyValueClient.class);

		expect(client.innerWriteKey(anyString(), anyString(), anyLong())).andReturn(true).once();
		replay(client);
		try {
			ArrayList<String> files = populateServer(server);

			server.registerClient("foo", 90004, client);

			String file = files.get(0).toString();
			long xid = server.startNewTransaction();
			server.setInTransaction(file, "zz", xid);
		} finally {
			server.cacheDisconnect("foo", 90004);
		}
		verify(client);
	}

	@Test
	public void testServerCommitReachesAllClients() throws Exception {

		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		KeyValueClient[] clients = new KeyValueClient[N_REPLICAS];
		long xid = server.startNewTransaction();
		String uniqueContent = "testServerCommitReachesAllClients." + System.currentTimeMillis();
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(KeyValueClient.class);
			for (int j = 0; j < files.size(); j++) {
				expect(clients[i].innerWriteKey(files.get(j).toString(), uniqueContent + files.get(j).toString(), xid)).andReturn(true).once();
			}
			clients[i].commitTransaction(xid);
			expectLastCall().once();
			server.registerClient("foo", 9000 + i, clients[i]);
			replay(clients[i]);
		}

		try {
			for (String p : files) {
				String file = p.toString();
				server.setInTransaction(file, uniqueContent + file, xid);
			}
			server.issueCommitTransaction(xid);
		} finally {
		}
		//Make sure that all of the writes went through on the server too
		KeyValueClient fake = mock(KeyValueClient.class);
		HashMap<String, String> endFiles = server.registerClient("fake", 9, fake);
		for (Map.Entry<String, String> e : endFiles.entrySet()) {
			String expected = uniqueContent + e.getKey();
			if (!e.getValue().equals(expected))
				fail("Writes were not committed on the server, expected file content " + expected + " but got " + e.getValue());
		}
		for (KeyValueClient client : clients)
			verify(client);
	}

	@Test
	public void testServerThatClientCantRegisterDuringCommit() throws Exception {
		err = false;
		dangling.clear();
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		AtomicInteger completedCommits = new AtomicInteger();
		FirstGuyLeavesWhileWritingClient c1 = new FirstGuyLeavesWhileWritingClient(server,9004,completedCommits);
		FirstGuyLeavesWhileWritingClient c2 = new FirstGuyLeavesWhileWritingClient(server,9005,completedCommits);

		server.registerClient("fake client", 9004, c1);
		server.registerClient("fake client", 9005, c2);

		long id = server.startNewTransaction();
		server.setInTransaction(files.get(0).toString(),"someContent",id);
		server.issueCommitTransaction(id);

		for(Thread t: dangling){
			t.join(100);
		}
		assertFalse(err);
	}
	HashSet<Thread> dangling = new HashSet<>();

	class FirstGuyLeavesWhileWritingClient extends NoopKeyValueClient {
		AtomicInteger completedCommits;
		AbstractKeyValueServer server;
		int port;

		FirstGuyLeavesWhileWritingClient(AbstractKeyValueServer server, int port, AtomicInteger completedCommits) {
			super(server);
			this.completedCommits = completedCommits;
			this.server = server;
			this.port = port;
		}

		@Override
		public boolean innerWriteKey(String file, String content, long xid) {
			return true;
		}

		@Override
		public void commitTransaction(long id) {
			Thread t = new Thread(() -> {
				AbstractKeyValueClient newClient = createNiceMock(AbstractKeyValueClient.class);
				try {
					expect(newClient.innerWriteKey(anyString(),anyString(),anyLong())).andReturn(true);
					replay(newClient);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					//Concurrency: this method can't return until the writes are all completed
					server.registerClient("fake", port+10, newClient);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (completedCommits.get() != 2) {
					err = true;
					System.err.println("Error: We were able to add a replica while a write was in progress");
				}
			});
			t.start();
			dangling.add(t);
			try {
				Thread.sleep(300);
				completedCommits.incrementAndGet();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
