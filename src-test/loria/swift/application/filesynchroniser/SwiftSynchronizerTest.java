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

import java.util.logging.Logger;
import loria.swift.application.filesystem.mapper.RegisterFileContent;
import loria.swift.crdt.logoot.LogootDocument;
import loria.swift.crdt.logoot.LogootVersioned;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import sys.Sys;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SwiftSynchronizerTest {
     private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("loria.swift.application.filesynchroniser");
    //TxnHandle txn;
    static Swift server;
    public SwiftSynchronizerTest() {
     
    }
    /**
     *
     */
    
    @BeforeClass
    public static void setUp()throws NetworkException {
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
        server = SwiftImpl.newInstance(scoutName, DCConstants.SURROGATE_PORT);

        
        // try {
       // txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        
    }
    @Test
    public void testLogoot() {
        SwiftSynchronizer sync=new SwiftSynchronizer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, false, LogootVersioned.class);
        sync.commit("test", "123");
        assertEquals("123",sync.update("test"));
    }
    
    @Test
    public void testLastWriterWin() {
        SwiftSynchronizer sync=new SwiftSynchronizer(server,IsolationLevel.SNAPSHOT_ISOLATION,  CachePolicy.STRICTLY_MOST_RECENT, true, false, RegisterFileContent.class);
        sync.commit("test2", "123");
        assertEquals("123",sync.update("test2"));
    }
}
