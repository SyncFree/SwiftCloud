/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.rc.tools;

import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.jobs.ClientModifierBenchmarkJob;
import loria.swift.application.filesynchroniser.SwiftSynchronizerServer;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.dc.DCConstants;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.Networking;

/**
 *
 * @author Stephane Martin <stephane@stephanemartin.fr>
 */
public class RunProxy {
    public static void main(String ...arg){
         if (arg.length < 3) {
            System.out.println("<host> [algo] [async]");
            return;
        }
        Sys.init();
        Logger.getLogger("").setLevel(Level.SEVERE);
        Logger.getLogger("loria.rc").setLevel(Level.ALL);
        ClientModifierBenchmarkJob.Type t = ClientModifierBenchmarkJob.Type.Logout;
        boolean aSync = false;
        if (arg.length > 1) {
            if (arg[1].contains("emote")) {
                t = ClientModifierBenchmarkJob.Type.Remote;
            } else if (!arg[1].contains("ogout")) {
                t = ClientModifierBenchmarkJob.Type.LastWriterWin;
            }
        }
        if (arg.length > 2 && !arg[2].contains("alse")) {
            aSync = true;
        }
        
        Endpoint dcEndpoint = Networking.Networking.resolve(arg[0] , DCConstants.SURROGATE_PORT);
           SwiftSession server= SwiftImpl
                    .newSingleSessionInstance(new SwiftOptions(dcEndpoint.getHost(), dcEndpoint.getPort()));
        SwiftSynchronizerServer serv=new SwiftSynchronizerServer(server, IsolationLevel.REPEATABLE_READS
               , CachePolicy.CACHED
                , false, aSync
                , ClientModifierBenchmarkJob.classes[t.ordinal()]);
        serv.start();
        System.out.println("Ready ...");
    }
}
