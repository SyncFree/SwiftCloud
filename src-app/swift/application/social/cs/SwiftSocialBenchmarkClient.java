package swift.application.social.cs;

import static sys.net.api.Networking.Networking;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import swift.application.social.Commands;
import swift.application.social.SwiftSocialMain;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Threading;
/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkClient {
	private static PrintStream bufferedOutput;
	private static String fileName = "scripts/commands.txt";
	private static int concurrentSessions;

	static Endpoint socialServer;
	
	static AtomicInteger commandsDone = new AtomicInteger(0);
	static AtomicInteger totalCommands = new AtomicInteger(0);
	
	public static void main(String[] args) {
		if (args.length < 3) {
			exitWithUsage();
		}
		final String server = args[0];
		fileName = args[1];
		concurrentSessions = Integer.valueOf(args[2]);

		final String shepardAddress = Args.valueOf(args, "-shepard", "");
		
		sys.Sys.init();
		
		socialServer = Networking.resolve(server, SwiftSocialBenchmarkServer.PORT);
		
		bufferedOutput = new PrintStream(System.out, false);
		bufferedOutput.println("session_id,command,command_exec_time,time");

		// Read all sessions from the file.
		final List<List<String>> sessions = readSessionsCommands(fileName, 0, Integer.MAX_VALUE);

		// Kick off all sessions, throughput is limited by
		// concurrentSessions.
		final ExecutorService sessionsExecutor = Executors.newFixedThreadPool(concurrentSessions);

        if( ! shepardAddress.isEmpty() ) 
            new Shepard().joinHerd(shepardAddress);

		System.err.println("Spawning session threads.");
		for (int i = 0; i < sessions.size(); i++) {
			final int sessionId = i;
			final List<String> commands = sessions.get(i);
			sessionsExecutor.execute(new Runnable() {
				public void run() {

				        if( shepardAddress.isEmpty() ) 
	                        runClientSession(sessionId, commands);
				        else 
				            for(;;)
	                            runClientSession(sessionId, commands);
				}
			});
		}
		// Wait for all sessions.
		sessionsExecutor.shutdown();
		try {
			sessionsExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.err.println("Session threads completed.");
		System.exit(0);
	}

	private static void exitWithUsage() {
		System.out.println("Usage 1: init <surrogate addr> <users filename>");
		System.out.println("With the last option being true, input is treated as list of users to populate db.");
		System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
		System.out.println("Usage 2: run <surrogate addr> <commands filename> <isolation level> <cache policy> <cache time eviction ms> <subscribe updates (true|false)> <async commit (true|false)>");
		System.out.println("         <think time ms> <concurrent sessions>");
		System.out.println("With the last option being true, input is treated as list of users to populate db.");
		System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
		System.exit(1);
	}

	private static void runClientSession(final int sessionId, final List<String> commands) {

		RpcEndpoint endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

		totalCommands.addAndGet(commands.size());
		final long sessionStartTime = System.currentTimeMillis();
		final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);

		bufferedOutput.println(initSessionLog);
		for (String cmdLine : commands) {
            String[] toks = cmdLine.split(";");
			final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
			final long txnStartTime = System.currentTimeMillis();
			
			final AtomicBoolean done = new AtomicBoolean(false);
			endpoint.send( socialServer, new Request( cmdLine ), new RequestHandler() {
				public void onReceive(Request m) {
					done.set(true);
					final long now = System.currentTimeMillis();
					final long txnExecTime = now - txnStartTime;
					final String log = String.format("%d,%s,%d,%d", sessionId, cmd, txnExecTime, now);
					bufferedOutput.println(log);
					commandsDone.incrementAndGet();
				}				
			});
			while( ! done.get() )
				Threading.sleep(10);
		}
		final long now = System.currentTimeMillis();
		final long sessionExecTime = now - sessionStartTime;
		bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
		bufferedOutput.flush();
		System.err.println("> " + IP.localHostname() + " all sessions completed...");
	}

	/**
	 * Reads sessions [firstSession, firstSession + sessionsNumber) from the
	 * file. Indexing starts from 0.
	 */
	private static List<List<String>> readSessionsCommands(final String fileName, final int firstSession, final int sessionsNumber) {
		final List<String> cmds = SwiftSocialMain.readInputFromFile(fileName);
		final List<List<String>> sessionsCmds = new ArrayList<List<String>>();

		List<String> sessionCmds = new ArrayList<String>();
		for (int currentSession = 0, currentCmd = 0; currentSession < firstSession + sessionsNumber && currentCmd < cmds.size(); currentCmd++) {
			final String cmd = cmds.get(currentCmd);
			if (currentSession >= firstSession) {
				sessionCmds.add(cmd);
			}

			final String[] toks = cmd.split(";");
			final Commands cmdType = Commands.valueOf(toks[0].toUpperCase());
			if (cmdType == Commands.LOGOUT) {
				if (currentSession >= firstSession && currentSession < firstSession + sessionsNumber) {
					sessionsCmds.add(sessionCmds);
				}
				sessionCmds = new ArrayList<String>();
				currentSession++;
			}
		}
		return sessionsCmds;
	}
}
