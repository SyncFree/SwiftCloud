/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.rc.tools;

import java.util.logging.Level;
import java.util.logging.Logger;
import loria.rc.jobs.SwiftCloudServerJob;

/**
 *
 * @author Stephane Martin <stephane@stephanemartin.fr>
 */
public class RunSwiftServer {

    public static void main(String... arg) {
        Logger.getLogger("").setLevel(Level.SEVERE);
        SwiftCloudServerJob job=new SwiftCloudServerJob();
        job.run();
    }
}
