package swift.application.social.cdn;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.org.apache.bcel.internal.generic.GETSTATIC;

import swift.application.social.Commands;
import swift.application.social.SwiftSocial;
import swift.application.social.SwiftSocialMain;
import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;
import sys.scheduler.PeriodicTask;
import sys.utils.Threading;
import swift.application.social.Message;

import static sys.Sys.*;
import static sys.net.api.Networking.Networking;
/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. OSDI 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmarkServer {
	public static int PORT = 11111;
	
    private static String dcName;
    private static String fileName = "scripts/commands.txt";
    private static IsolationLevel isolationLevel;
    private static CachePolicy cachePolicy;
    private static boolean subscribeUpdates;
    private static boolean asyncCommit;
    private static long cacheEvictionTimeMillis;
    private static long thinkTime;
    
    public static void main(String[] args) {
        if (args.length < 3) {
            exitWithUsage();
        }
        final String command = args[0];
        dcName = args[1];
        fileName = args[2];
        sys.Sys.init();

        if (command.equals("init") && args.length == 3) {
            System.out.println("Populating db with users...");
            final SwiftImpl swiftClient = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
            final SwiftSocial socialClient = new SwiftSocial(swiftClient, IsolationLevel.REPEATABLE_READS,
                    CachePolicy.CACHED, false, false);
            SwiftSocialMain.initUsers(swiftClient, socialClient, fileName);
            swiftClient.stop(true);
            System.out.println("Finished populating db with users.");
            System.exit(0);            
        } else if (command.equals("run") && args.length == 10) {
            isolationLevel = IsolationLevel.valueOf(args[3]);
            cachePolicy = CachePolicy.valueOf(args[4]);
            cacheEvictionTimeMillis = Long.valueOf(args[5]);
            subscribeUpdates = Boolean.parseBoolean(args[6]);
            asyncCommit = Boolean.parseBoolean(args[7]);
            thinkTime = Long.valueOf(args[8]);            
        }
        
        Networking.rpcBind(PORT, TransportProvider.DEFAULT).toService(0, new CdnRpcHandler() {
				
				@Override
				public void onReceive(final RpcHandle handle, final CdnRpc m) {
					String sessionId = handle.remoteEndpoint().toString();
					List<String> cmds = new ArrayList<String>();
					cmds.add( m.payload );
					execCommands(sessionId, cmds );
					handle.reply( new CdnRpc("OK") );
				}				
			});

//        System.err.println("SwiftSocial Server Ready...");
     }

    private static void exitWithUsage() {
        System.out.println("Usage 1: init <surrogate addr> <users filename>");
        System.out.println("With the last option being true, input is treated as list of users to populate db.");
        System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
        System.out
                .println("Usage 2: run <surrogate addr> <commands filename> <isolation level> <cache policy> <cache time eviction ms> <subscribe updates (true|false)> <async commit (true|false)>");
        System.out.println("         <think time ms> <concurrent sessions>");
        System.out.println("With the last option being true, input is treated as list of users to populate db.");
        System.out.println("Without the last options, input is treated as list of sessions with commands to run.");
        System.exit(1);
    }

    private static void execCommands( String sessionId, Collection<String> commands ) {
    	SwiftSocial socialClient = getSession( sessionId ).swiftSocial;
    	
        for (String cmdLine : commands) {
            String[] toks = cmdLine.split(";");
            final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
            switch (cmd) {
            case LOGIN:
                if (toks.length == 3) {
                    socialClient.login(toks[1], toks[2]);
                    break;
                }
            case LOGOUT:
                if (toks.length == 2) {
                    socialClient.logout(toks[1]);
                    break;
                }
            case READ:
                if (toks.length == 2) {
                    socialClient.read(toks[1], new HashSet<Message>(), new HashSet<Message>());
                    break;
                }
            case SEE_FRIENDS:
                if (toks.length == 2) {
                    socialClient.readFriendList(toks[1]);
                    break;
                }
            case FRIEND:
                if (toks.length == 2) {
                    socialClient.befriend(toks[1]);
                    break;
                }
            case STATUS:
                if (toks.length == 2) {
                    socialClient.updateStatus(toks[1], System.currentTimeMillis());
                    break;
                }
            case POST:
                if (toks.length == 3) {
                    socialClient.postMessage(toks[1], toks[2], System.currentTimeMillis());
                    break;
                }
            default:
                System.err.println("Can't parse command line :" + cmdLine);
                System.err.println("Exiting...");
                System.exit(1);
            }
            if (thinkTime > 0) {
                try {
                    Thread.sleep(thinkTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    static Session getSession( String sessionId ) {
    	Session res = sessions.get( sessionId );
    	if( res == null ) {
    		sessions.put( sessionId, res = new Session() );
    	}
    	return res;
    }
    
    static Map<String, Session> sessions = new HashMap<String, Session>();
    
    static class Session {
    	final Swift swiftCLient;
    	final SwiftSocial swiftSocial;
    	
    	Session() {
    		swiftCLient = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT, SwiftImpl.DEFAULT_TIMEOUT_MILLIS,
	                Integer.MAX_VALUE, cacheEvictionTimeMillis);

    		swiftSocial = new SwiftSocial(swiftCLient, isolationLevel, cachePolicy, subscribeUpdates,
    	                asyncCommit);
    	}
    }
}
