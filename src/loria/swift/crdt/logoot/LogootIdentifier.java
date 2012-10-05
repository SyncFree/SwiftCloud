/**
 * Replication Benchmarker
 * https://github.com/score-team/replication-benchmarker/
 * Copyright (C) 2012 LORIA / Inria / SCORE Team
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package loria.swift.crdt.logoot;

import java.io.Serializable;
import java.util.*;
import swift.clocks.TripleTimestamp;

/**
 * Logoot identifier. A list of Component.
 * @author urso
 */
public class LogootIdentifier implements Comparable<LogootIdentifier>, Serializable {

    final private ArrayList<Component> id;

    public LogootIdentifier(int capacity) {
        id = new ArrayList<Component>(capacity);
    }

    LogootIdentifier(Component component) {
        id = new ArrayList<Component>(1);
        id.add(component);
    }

    public ArrayList<Component> getID() {
        return id;
    }

    public Component getComponentAt(int position) {
        return id.get(position);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogootIdentifier other = (LogootIdentifier) obj;
        if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

    public void addComponent(Component cp) {
        id.add(cp);
    }

    public int length() {
        return id.size();
    }

    @Override
    public String toString() {
        String ligneIdentif = "";
        for (Component c : id) {
            ligneIdentif += c.toString();
        }
        return ligneIdentif;
    }

    /**
     * Returns O if j > index().
     **/
    long getDigitAt(int index) {
        if (index >= this.length()) {
            return 0;
        } else {
            return id.get(index).getDigit();
        }
    }

    /**
     * Digits of this identifier until index included (filled by 0s if index >= length()) 
     */
    public List<Long> digits(int index) {
        List<Long> l = new ArrayList<Long>();
        for (int i = 0; i <= index; i++) {
            if (i >= id.size()) {
                l.add(0L);
            } else {
                l.add(id.get(i).getDigit());
            }
        }
        return l;
    }

    @Override
    public int compareTo(LogootIdentifier t) {
        int m = Math.min(id.size(), t.id.size());
        for (int i = 0; i < m; i++) {
            int c = id.get(i).compareTo(t.id.get(i));
            if (c != 0) {
                return c;
            }
        }
        return id.size() - t.id.size();
    }

    @Override
    public LogootIdentifier clone() {
        LogootIdentifier o = new LogootIdentifier(id.size());
        for (Component c : id) {
            o.id.add(c.clone());
        }
        return o;
    }

    public Component getLastComponent() {
        return id.get(id.size()-1);
    }
    
    void setComponent(int i, Component c) {
        id.set(i, c);
    }
}
