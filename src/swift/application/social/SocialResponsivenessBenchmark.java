package swift.application.social;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import sys.Sys;

/**
 * Benchmark of SwiftSocial responsiveness, based on data model derived from
 * WaltSocial prototype [Sovran et al. OSDI 2011].
 */
public class SocialResponsivenessBenchmark {
    private static final long DELAY_AFTER_INIT = 3000;
    private static int lengthInputFile = 500;
    private static String dcName;
    private static String usersFileName;
    private static String commandsFileName = "scripts/commands.txt";
    private static IsolationLevel isolationLevel;
    private static CachePolicy cachePolicy;
    private static boolean subscribeUpdates;

    public static void main(String[] args) {
        if (args.length != 5 && args.length != 6) {
            System.out
                    .println("Usage: <surrogate addr> <isolationLevel> <cachePolicy> <subscribe updates (true|false)>");
            System.out
                    .println("       <commands filename> <think time avg> <think time stdev> <cache time eviction ms>");
            return;
        } else {
            dcName = args[0];
            isolationLevel = IsolationLevel.valueOf(args[1]);
            cachePolicy = CachePolicy.valueOf(args[2]);
            subscribeUpdates = Boolean.parseBoolean(args[3]);
            commandsFileName = args[4];
            if (args.length == 6) {
                usersFileName = args[5];
            } else {
                usersFileName = null;
            }
        }
        runClient(commandsFileName, usersFileName);
    }

    private static List<String> readInputFromFile(final String fileName) {
        List<String> data = new ArrayList<String>(lengthInputFile);
        try {
            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            try {
                while ((strLine = br.readLine()) != null) {
                    // read file into memory
                    data.add(strLine);
                }
            } finally {
                in.close();
            }
            return data;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void runClient(final String inputFileName) {
        Sys.init();
        Swift clientServer = SwiftImpl.newInstance(dcName, DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer, isolationLevel, cachePolicy, subscribeUpdates);

        // Execute the commands assigned to this thread
        List<String> commandData = readInputFromFile(inputFileName);
        while (true) {
            int n_fail = 0;
            for (String line : commandData) {
                String[] toks = line.split(";");
                switch (Commands.valueOf(toks[0].toUpperCase())) {
                case LOGIN:
                    if (toks.length == 3) {
                        client.login(toks[1], toks[2]);
                        break;
                    }
                case LOGOUT:
                    if (toks.length == 2) {
                        client.logout(toks[1]);
                        break;
                    }
                case READ:
                    if (toks.length == 2) {
                        client.read(toks[1], new HashSet<Message>(), new HashSet<Message>());
                        break;
                    }
                case SEE_FRIENDS:
                    if (toks.length == 2) {
                        client.readFriendList(toks[1]);
                        break;
                    }
                case FRIEND:
                    if (toks.length == 2) {
                        client.befriend(toks[1]);
                        break;
                    }
                case STATUS:
                    if (toks.length == 2) {
                        client.updateStatus(toks[1], System.currentTimeMillis());
                        break;
                    }
                case POST:
                    if (toks.length == 3) {
                        client.postMessage(toks[1], toks[2], System.currentTimeMillis());
                        break;
                    }
                default:
                    System.out.println("Can't parse command line :" + line);
                    n_fail++;
                }
            }
        }

    }

    private static Map<String, List<String>> readUsersCommands(final String fileName) {
        final List<String> cmds = readInputFromFile(fileName);
        final Map<String, List<String>> usersCmds = new HashMap<String, List<String>>();

        List<String> userCmds = null;
        for (final String cmd : cmds) {
            final String[] toks = cmd.split(";");
            final Commands cmdType = Commands.valueOf(toks[0].toUpperCase());
            switch (cmdType) {
            case LOGIN:
                userCmds = new ArrayList<String>();
                usersCmds.put(toks[1], userCmds);
                break;
            case LOGOUT:
                userCmds.add(cmd);
                break;
            }
        }
        return usersCmds;
    }

    // private static void startDCServer() {
    // DCServer.main(new String[] { sequencerName });
    // }
    //
    // private static void startSequencer() {
    // DCSequencerServer.main(new String[] { "-name", sequencerName });
    // }

}
