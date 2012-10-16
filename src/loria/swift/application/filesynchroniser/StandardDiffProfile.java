/**
 * Replication Benchmarker
 * https://github.com/score-team/replication-benchmarker/ Copyright (C) 2012
 * LORIA / Inria / SCORE Team
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package loria.swift.application.filesynchroniser;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * A profile that generates operation similar to VCS diff (block of lines).
 *
 * @author urso
 */
public class StandardDiffProfile {

    private enum OpType {

        update, ins, del
    };
    private final double perUp, perIns, perBlock, sdvBlockSize, sdvLineSize;
    private final double avgBlockSize, avgLinesize, avgDelSize, sdvDelSize;
    private final RandomGauss random;
    public static final StandardDiffProfile GIT = new StandardDiffProfile(0.69, 0.74, 1, 32.6, 137.9, 1.5, 5.8, 40, 20.0);
    public static final StandardDiffProfile BASIC = new StandardDiffProfile(0.7, 0.7, 0.9, 6, 5.0, 1, 5.8, 30, 10.0);
    public static final StandardDiffProfile SMALL = new StandardDiffProfile(0.7, 0.7, 0.9, 5, 1.0, 2, 5.8, 10, 3.0);
    public static final StandardDiffProfile WITHOUT_BLOCK = new StandardDiffProfile(0.7, 0.7, 0, 1, 0, 2, 5.8, 30, 10.0);

    /**
     * Constructor of profile
     *
     * @param perUp percentage of update vs other operation
     * @param perIns percentage of ins vs del operation
     * @param perBlock percentage of block operation (size >= 1)
     * @param avgBlockSize average size of block operation (in number of lines)
     * @param sdvBlockSize standard deviation of block operations' size
     * @param avgLinesize average line size
     * @param sdvLineSize standard deviation of line's size
     */
    private StandardDiffProfile(double perUp, double perIns, double perBlock, double avgBlockSize, double sdvBlockSize, double avgDelSize, double sdvDelSize, int avgLinesize, double sdvLineSize) {
        this.perUp = perUp;
        this.perIns = perIns;
        this.perBlock = perBlock;
        this.avgBlockSize = avgBlockSize;
        this.sdvBlockSize = sdvBlockSize;
        this.avgDelSize = avgDelSize;
        this.sdvDelSize = sdvDelSize;
        this.avgLinesize = avgLinesize;
        this.sdvLineSize = sdvLineSize;
        this.random = new RandomGauss();
    }

    private OpType nextType() {
        return (random.nextDouble() < perUp) ? OpType.update
                : (random.nextDouble() < perIns) ? OpType.ins : OpType.del;
    }

    private String nextContent() {
        StringBuilder s = new StringBuilder();
        int length = (random.nextDouble() < perBlock)
                ? (int) random.nextLongGaussian(avgBlockSize, sdvBlockSize) : 1;
        for (int i = 0; i < length; i++) {

            int lineSize = (int) random.nextLongGaussian(avgLinesize, sdvLineSize);
            for (int j = 0; j < lineSize; j++) {
                s.append((char) ('a' + random.nextInt(26)));
            }
            s.append('\n');
        }
        return s.toString();
    }

    private int nextOffset(int position, int l) {
        int length = (random.nextDouble() < perBlock)
                ? (int) random.nextLongGaussian(avgDelSize, sdvDelSize) : 1;
        return Math.min(l - position, length);
    }

    public int nextPosition(int length) {
        return random.nextInt(length);
    }

    private String nextOperation(String text) {
        int l = text.length();
        String t2;
        int position = l == 0 ? 0 : nextPosition(l);
        OpType type = (l == 0) ? OpType.ins : nextType();
        int offset = (type == OpType.ins) ? 0 : nextOffset(position, l);
        String content = (type == OpType.del) ? null : nextContent();

        if (type == OpType.ins) {
            t2 = text.substring(0, position) + content;
            if (position + 1 < l) {
                t2 = text.substring(position + 1, l);
            }
        } else if (type == OpType.del) {
            t2 = text.substring(0, position) + text.substring(position + offset, l);
        } else {
            t2 = text.substring(0, position) + content + text.substring(position + offset, l);
        }
        return t2;
    }

    /**
     * Change randomly a text.
     */
    public String change(String text) {
        int nbOp = random.nextInt(3) + 1;
        for (int o = 0; o < nbOp; ++o) {
            text = nextOperation(text);
        }
        return text;
    }

    /**
     * For testing since random cannot be junit tested.
     *
     * @param args
     */
    public static void main(String... args) {
        StandardDiffProfile d = StandardDiffProfile.GIT;
        String text = "";
        for (int i = 0; i < 60; i++) {
            text = d.change(text);
            System.out.println("---\n" + text.length());
        }

    }
}
