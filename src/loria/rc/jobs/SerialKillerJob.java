/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package loria.rc.jobs;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * this jobs shuts down the machine
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SerialKillerJob extends Jobs {

    @Override
    public void run() {
        kill();
    }

    public static void kill() {
        try {
            Process p = Runtime.getRuntime().exec("sudo poweroff");
        } catch (IOException ex) {
            Logger.getLogger(SerialKillerJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
