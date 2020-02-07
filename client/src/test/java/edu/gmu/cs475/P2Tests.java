package edu.gmu.cs475;

import edu.gmu.cs475.internal.CaptureMatcher;
import edu.gmu.cs475.internal.ServerMain;
import org.easymock.Capture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static edu.gmu.cs475.P1Tests.populateServer;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Created by jon on 3/19/18.
 */
public class P2Tests {

	private static final int N_FILES = 20;
	private static final int N_REPLICAS = 20;
	@Rule
	public Timeout globalTimeout = new Timeout(22000);
	boolean err;

	@Test
	public void testServerManyClientsRegisterAllGetInnerWriteAndCommit() throws Exception {
		//Get a real server
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		//Set up fake clients
		KeyValueClient[] clients = new KeyValueClient[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWriteAndCommit." + System.currentTimeMillis() + ".";
		HashSet<Capture<Long>> xids = new HashSet<>();
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(KeyValueClient.class);
			xids.clear();
			for (String p : files) {
				Capture<Long> xid = newCapture();
				xids.add(xid);
				expect(clients[i].innerWriteKey(eq(p.toString()), eq(contentToWrite + p.toString()), captureLong(xid))).andReturn(true);
				clients[i].commitTransaction(CaptureMatcher.matchesCapturedLong(xid));
				expectLastCall().once();
			}
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
		HashSet<Long> actualXids = new HashSet<>();
		for (Capture<Long> xid : xids)
			if (!actualXids.add(xid.getValue()))
				fail("Expected each write would get a unique transaction ID, but observed " + xid.getValue() + " more than once");
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
	public void testServersetPropogatesErrorsAndAborts() throws Exception {
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
			clients[i].abortTransaction(anyLong());
			expectLastCall().once();

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
	public void testServersetPropogatesErrorsAndAbortsOnException() throws Exception {
		KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		//Set up fake clients
		IKeyValueReplica[] clients = new IKeyValueReplica[N_REPLICAS];
		String contentToWrite = "testServerManyClientsRegisterAllGetInnerWrite." + System.currentTimeMillis() + ".";
		for (int i = 0; i < N_REPLICAS; i++) {
			clients[i] = mock(IKeyValueReplica.class);
			if (i == N_REPLICAS / 2)
				expect(clients[i].innerWriteKey(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andThrow(new RemoteException("Mock IO excpetion")).times(1);
			else
				expect(clients[i].innerWriteKey(eq(files.get(0).toString()), eq(contentToWrite), anyLong())).andReturn(true).times(0, 1);
			clients[i].abortTransaction(anyLong());
			expectLastCall().once();

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
		for (IKeyValueReplica client : clients)
			verify(client);
	}


	@Test
	public void testAbortReturnsContentToNormalAndAnotherCommitWrites() throws Exception {

		final KeyValueServer server = new KeyValueServer();
		ArrayList<String> files = populateServer(server);
		final long TIMEOUT = 200;
		this.err = true;

		AtomicInteger nConcurrentWrites = new AtomicInteger();
		final String fileToCrashOn = files.get(0).toString();
		AbstractKeyValueClient writerWhoAborts = new KeyValueClient(server) {
			@Override
			public boolean innerWriteKey(String file, String content, long xid) {
				if (fileToCrashOn.equals(file))
					return false;
				return super.innerWriteKey(file, content, xid);
			}
		};
		AbstractKeyValueClient regularWriter = new KeyValueClient(server);

		String originalContent = regularWriter.get(files.get(0).toString());
		try {
			Thread writerThread = new Thread(() -> {
				boolean caught = false;
				try {
					server.set(files.get(0).toString(), "garbage");
				} catch (IOException e) {
					caught = true;
				}
				if (!caught) {
					err = true;
					System.err.println("Expected write to abort");
				}
			});

			Thread writer2Thread = new Thread(() -> {
				try {
					server.set(files.get(1).toString(), "garbage2");
				} catch (IOException e) {
					e.printStackTrace();
					err = true;
				}
			});
			writerThread.start();
			writer2Thread.start();
			writerThread.join();
			writer2Thread.join();
			
			assertEquals(originalContent, regularWriter.get(files.get(0).toString()));
			assertEquals(originalContent, writerWhoAborts.get(files.get(0).toString()));

			System.out.println("hey hi " + regularWriter.get(files.get(0).toString()));
			assertEquals("garbage2", regularWriter.get(files.get(1).toString()));
			assertEquals("garbage2", writerWhoAborts.get(files.get(1).toString()));

			//Last, check that the server has all of the right files.
			KeyValueClient fake = mock(KeyValueClient.class);
			HashMap<String, String> endFiles = server.registerClient("fake", 9, fake);
			assertEquals(originalContent,endFiles.get(files.get(0).toString()));
		} finally {
			regularWriter.cleanup();
			writerWhoAborts.cleanup();
		}
	}
}
