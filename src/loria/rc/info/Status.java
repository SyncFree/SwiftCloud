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
package loria.rc.info;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class Status extends Info {

    public static enum State {

        Finished, Ready
    };
    State state;
    public static final Status FINISHED = new Status(State.Finished);
    public static final Status READY = new Status(State.Ready);

    public Status(State state) {
        this.state = state;
    }

    public State getState() {
        return state;
    }
}
