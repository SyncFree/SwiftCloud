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
package loria.swift.application.filesynchroniser;

import java.net.InetAddress;
import java.util.logging.Level;
import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import loria.swift.application.filesystem.mapper.RegisterFileContent;
import loria.swift.crdt.logoot.LogootVersioned;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import sys.Sys;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SwiftSynchronizerClientServerTest {
     private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("loria.swift.application.filesynchroniser");
    //TxnHandle txn;
    static SwiftSession server;
    public SwiftSynchronizerClientServerTest() {
     
    }
    /**
     *
     */
    
    @BeforeClass
    public static void setUp()throws NetworkException {
        Logger log1= Logger.getLogger("");
        Logger log2= Logger.getLogger("loria");
        log1.setLevel(Level.SEVERE);
        log2.setLevel(Level.ALL);
           DCSequencerServer.main(new String[]{"-name", sequencerName});

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }

        DCServer.main(new String[]{sequencerName});
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Sys.init();
        server = SwiftImpl.newSingleSessionInstance(new SwiftOptions(scoutName, DCConstants.SURROGATE_PORT));

        
        // try {
       // txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        
    }
    
    void testCommitUpdate(SwiftSynchronizerServer serv,String name)throws Exception {
        serv.start();
         try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }
         System.out.println("Connecting");
         System.out.flush();
        SwiftSynchronizerClient sync=new SwiftSynchronizerClient(InetAddress.getLocalHost(),serv.getPort());
        System.out.println("send commit");
        System.out.flush();
        sync.commit(name, "1234");
        System.out.println("update");
        System.out.flush();
        
        assertEquals("1234", sync.update(name));
    }
    
    
    @Test
    public void testLogootAsync() throws Exception {
        SwiftSynchronizerServer serv=new SwiftSynchronizerServer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, true, LogootVersioned.class);
        testCommitUpdate(serv,"test1");
    }

    
    @Test
    public void testLastWriterWinAsync() throws Exception {
        SwiftSynchronizerServer serv=new SwiftSynchronizerServer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, true, RegisterFileContent.class);
        serv.setPort(serv.getPort()+1);
        testCommitUpdate(serv,"test2");
    }
    
    @Test
    public void testLogoot()  throws Exception{
        SwiftSynchronizerServer serv=new SwiftSynchronizerServer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, false, LogootVersioned.class);
        serv.setPort(serv.getPort()+2);
         testCommitUpdate(serv,"test3");
    }
    
    @Test
    public void testLastWriterWin() throws Exception {
       SwiftSynchronizerServer serv=new SwiftSynchronizerServer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, false, RegisterFileContent.class);
       serv.setPort(serv.getPort()+3);
        testCommitUpdate(serv,"test4");
    }
}
