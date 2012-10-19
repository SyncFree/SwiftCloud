/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.rc.tools;

import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.jobs.ClientModifierBenchmarkJob;
import sys.Sys;

/**
 *
 * @author Stephane Martin <stephane@stephanemartin.fr>
 */
public class RunClient {

    public static void main(String... arg) throws InterruptedException {
        if (arg.length < 2) {
            System.out.println("<host> <job> [algo] [async]");
            return;
        }
        Sys.init();
        Logger.getLogger("").setLevel(Level.SEVERE);
        Logger.getLogger("loria.rc").setLevel(Level.ALL);
        ClientModifierBenchmarkJob.Type t = ClientModifierBenchmarkJob.Type.Logout;
        boolean aSync = false;
        if (arg.length > 2) {
            if (arg[2].contains("emote")) {
                t = ClientModifierBenchmarkJob.Type.Remote;
            } else if (!arg[2].contains("ogout")) {
                t = ClientModifierBenchmarkJob.Type.LastWriterWin;
            }
        }
        if (arg.length > 3 && !arg[3].contains("alse")) {
            aSync = true;
        }


        ClientModifierBenchmarkJob clientJob =
                new ClientModifierBenchmarkJob(t,
                300, aSync, 0, 5);

        clientJob.setDestHostName(arg[0]);
        clientJob.setJobName(arg[1]);


        clientJob.run();
      //  clientJob.join();
        System.exit(0);
    }
}
