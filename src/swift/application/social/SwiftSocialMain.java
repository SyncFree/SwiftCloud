package swift.application.social;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class SwiftSocialMain {
    static String sequencerName = "localhost";
    static String inputFileName = "scripts/commands.txt";

    public static void main(String[] args) {
        startSequencer();
        startDCServer();
        runClient(inputFileName);
    }

    private static void runClient(String inputFileName) {
        Sys.init();
        int portId = 2001;
        Swift clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer);

        List<String> commands = new ArrayList<String>(500);
        try {
            FileInputStream fstream = new FileInputStream(inputFileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                // read file into memory
                commands.add(strLine);
            }
            // Close the input stream
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Execute the commands assigned to this thread
        int n_fail = 0;
        for (String line : commands) {
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
            case FRIEND:
                if (toks.length == 2) {
                    client.sendFriendRequest(toks[1]);
                }
            default:
                System.out.println("Can't parse command line :" + line);
                n_fail++;
            }
        }

    }

    private static void startDCServer() {
        Thread server = new Thread() {
            public void run() {
//                DCServer server = new DCServer(sequencerName);
//                server.startSurrogServer();
                DCServer.main( new String[] {sequencerName});
            }
        };
        server.start();
    }

    private static void startSequencer() {
        Thread sequencer = new Thread() {
            public void run() {
                DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
                sequencer.start();
            }
        };
        sequencer.start();
    }

}
