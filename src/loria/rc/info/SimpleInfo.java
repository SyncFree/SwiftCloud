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
 * It's a simple return packet for information.
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SimpleInfo extends Info {
public static enum TypeInfo {

        Warning, Error, Info
    };
    protected TypeInfo type=TypeInfo.Info;

    public TypeInfo getType() {
        return type;
    }

    public void setType(TypeInfo type) {
        this.type = type;
    }
    
    String info;

    public SimpleInfo(TypeInfo type, String info) {
        this.info = info;
        this.type = type;
    }

    @Override
    public String toString() {
        return type + " : " + info +'\n';
    }
    
}
