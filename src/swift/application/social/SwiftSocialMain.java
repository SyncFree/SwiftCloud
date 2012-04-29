package swift.application.social;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import swift.client.SwiftImpl;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class SwiftSocialMain {
    private static String sequencerName = "localhost";
    private static String usersFileName = "scripts/users.txt";
    private static String commandsFileName = "scripts/commands.txt";

    public static void main(String[] args) {
        startSequencer();
        startDCServer();
        runClient(commandsFileName, usersFileName);
    }

    private static List<String> readInputFromFile(final String fileName) {
        List<String> data = new ArrayList<String>(500);
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

    private static void runClient(final String inputFileName, final String usersFileName) {
        Sys.init();
        int portId = 2001;
        Swift clientServer = SwiftImpl.newInstance(portId, "localhost", DCConstants.SURROGATE_PORT);
        SwiftSocial client = new SwiftSocial(clientServer);

        // Initialize user data
        List<String> userData = readInputFromFile(usersFileName);
        for (String line : userData) {
            String[] toks = line.split(";");
            long birthday = 0;
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("dd/mm/yy");
                Date dateStr = formatter.parse(toks[4]);
                birthday = dateStr.getTime();
            } catch (ParseException e) {
                System.err.println("Could not parse the birthdate: " + toks[4]);
            }
            client.registerUser(toks[1], toks[2], toks[3], birthday, System.currentTimeMillis());
        }
        System.out.println("Initialization finished");

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

    private static void startDCServer() {
        DCServer.main(new String[] { sequencerName });
    }

    private static void startSequencer() {
        DCSequencerServer.main( new String[] { "-name", sequencerName});
//        DCSequencerServer sequencer = new DCSequencerServer(sequencerName);
//        sequencer.start();
    }

}
