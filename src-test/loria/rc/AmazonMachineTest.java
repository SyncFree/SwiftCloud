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
package loria.rc;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class AmazonMachineTest {
    
    
    public static AmazonMachine am;
    public AmazonMachineTest() {
    }

     @BeforeClass
     public static  void init() throws Exception{
          am=new AmazonMachine();
        
     }
    @Test
    public void testSomeMethod() throws Exception {
        am.checkAndCreateSecurityGroup();
        am.startInstanceRequest(3);
       // am.instanceIsLauched();
        am.waitAllLauched();
        System.out.println("Tada !");
       Thread.sleep(30000);
        //assertTrue(am.areAnyOpen());
       // Thread.sleep(60000);
        
        am.cleanupInstances();
        //am.cleanup();
    }
    @Ignore
    @Test 
    public void testCleanup(){
       
        am.cleanupInstances();
    }
    
}
