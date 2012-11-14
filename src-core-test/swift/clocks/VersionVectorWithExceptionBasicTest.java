/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
package swift.clocks;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;
import swift.clocks.VersionVectorWithExceptions.Interval;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class VersionVectorWithExceptionBasicTest {

    
   
    
    @Test
    public void registerListCompact() {
        VersionVectorWithExceptions clock = new VersionVectorWithExceptions();
        
        clock.record(new Timestamp("a", 1));
        clock.record(new Timestamp("a", 3));
        clock.record(new Timestamp("a", 8));
        clock.record(new Timestamp("a", 4));
        clock.record(new Timestamp("a", 7));
        clock.record(new Timestamp("a", 2));
        clock.record(new Timestamp("a", 9));
        
        
        
        assertEquals(Arrays.asList(new Interval(1,4),new Interval(7,9)),clock.vv.get("a"));
    }
     @Test
    public void registerListCompactMerge() {
        VersionVectorWithExceptions clock = new VersionVectorWithExceptions();
        VersionVectorWithExceptions clock2 = new VersionVectorWithExceptions();
        
        clock.record(new Timestamp("a", 1));
        clock.record(new Timestamp("a", 3));
        clock.record(new Timestamp("a", 8));
        clock.record(new Timestamp("a", 4));
        clock.record(new Timestamp("a", 7));
        clock.record(new Timestamp("a", 2));
        clock.record(new Timestamp("a", 9));
       
        clock2.record(new Timestamp("a", 6));
        clock2.record(new Timestamp("a", 9));
      
        clock2.merge(clock);
      
        assertEquals(Arrays.asList(new Interval(1,4),new Interval(6,9)),clock2.vv.get("a"));
    }
     
    @Test
    public void testMaxListCovers() {
        VersionVectorWithExceptions clock  = new VersionVectorWithExceptions();
        VersionVectorWithExceptions clock2  = new VersionVectorWithExceptions();
        
        List<Interval> l1 = Arrays.asList(new Interval(1, 3));
        List<Interval> l2 = Arrays.asList(new Interval(2, 2));
        clock.vv.put("a", new LinkedList(l1));
        clock2.vv.put("a", new LinkedList(l2));
        
        clock2.merge(clock);
        
        assertEquals(l1,clock2.vv.get("a"));


        
    }

    @Test
    public void testMergeSplitedList() {
        List<Interval> l1 = Arrays.asList(new Interval(1, 1), new Interval(4, 5), new Interval(20, 20));
        List<Interval> l2 = Arrays.asList(new Interval(2, 3), new Interval(6, 6), new Interval(9, 10), new Interval(19,19));
        VersionVectorWithExceptions clock  = new VersionVectorWithExceptions();
        VersionVectorWithExceptions clock2  = new VersionVectorWithExceptions();
        clock.vv.put("a", new LinkedList(l1));
        clock2.vv.put("a", new LinkedList(l2));
        List<Interval> exp = Arrays.asList(new Interval(1, 6),new Interval(9,10),new Interval(19,20));
        clock2.merge(clock);
        assertEquals(exp,clock2.vv.get("a")); 

      


        
    }

    
}
