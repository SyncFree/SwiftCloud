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
package swift.application.filesystem;

import fuse.FuseGetattrSetter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;
import org.junit.Test;
import swift.client.SwiftImpl;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import static org.junit.Assert.*;
import swift.application.filesystem.fuse.FilesystemFuse;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FilesystemBasicTest {

    public FilesystemBasicTest() {
    }
    private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("swift.filesystem");

    @Test
    public void testSomeMethod() throws Exception {
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
        Swift server = SwiftImpl.newInstance(scoutName, DCConstants.SURROGATE_PORT);

        TxnHandle txn;
        // try {
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

        // create a root directory
        logger.info("Creating file system");
        Filesystem fs = new FilesystemBasic(txn, "test", "DIR");
        txn.commit();

        logger.info("Creating directories and subdirectories");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        fs.createDirectory(txn, "testfs1", "/test");
        fs.createDirectory(txn, "testfs2", "/test");

        fs.createDirectory(txn, "include", "/test/testfs1");
        fs.createDirectory(txn, "sys", "/test/testfs1/include");
        fs.createDirectory(txn, "netinet", "/test/testfs1/include");
        assertTrue(fs.isDirectory(txn,"include", "/test/testfs1"));
        assertTrue(fs.isDirectory(txn,"sys", "/test/testfs1/include"));
        assertTrue(fs.isDirectory(txn,"netinet", "/test/testfs1/include"));
        
        txn.commit();

        
        
        
        
        logger.info("Creating a file");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        IFile f1 = fs.createFile(txn, "file1.txt", "/test/testfs1");
        String s = "This is a test file";
        // assert (s.equals(new String(s.getBytes())));
        f1.reset(s.getBytes());
        System.out.println("Expected: " + s);
        System.out.println("Got: " + new String(f1.getBytes()));

        assert (new String(f1.getBytes()).equals(s));
        fs.updateFile(txn, "file1.txt", "/test/testfs1", f1);
        txn.commit();

        logger.info("Reading from the file");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        IFile f1_up = fs.readFile(txn, "file1.txt", "/test/testfs1");
        assert (Arrays.equals(f1_up.getBytes(), s.getBytes()));
        txn.commit();
 
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        assertTrue(fs.isFile(txn, "file1.txt", "/test/testfs1"));
        assertTrue(fs.isDirectory(txn,"testfs2", "/test"));
        txn.commit();
        
        
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        logger.info("Updating the file");
        String prefix = "Yes! ";
        byte[] concat = (prefix + s).getBytes();

        ByteBuffer buf_up = ByteBuffer.wrap(concat);
        f1_up.update(buf_up, 0);
        assert (Arrays.equals(f1_up.getBytes(), concat));
        fs.updateFile(txn, "file1.txt", "/test/testfs1", f1_up);
        txn.commit();

        logger.info("Checking that updates are committed");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        IFile f1_upp = fs.readFile(txn, "file1.txt", "/test/testfs1");
        assert (Arrays.equals(f1_upp.getBytes(), concat));
        txn.commit();

        logger.info("Copying the file");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        fs.copyFile(txn, "file1.txt", "/test/testfs1", "/test/testfs2");
        IFile f1_copy = fs.readFile(txn, "file1.txt", "/test/testfs2");
        assert (Arrays.equals(f1_copy.getBytes(), concat));
        txn.commit();

        logger.info("Removing the file");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        fs.removeFile(txn, "file1.txt", "/test/testfs1");
        txn.commit();

        logger.info("Recreating the file");
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        f1 = fs.createFile(txn, "file1.txt", "/test/testfs1");
        s = "This is a test file2";
        // assert (s.equals(new String(s.getBytes())));
        f1.reset(s.getBytes());
        System.out.println("------------------------------------------");
        System.out.println("Expected: " + s);
        System.out.println("Got: " + new String(f1.getBytes()));
        assertTrue(fs.isFile(txn, "file1.txt", "/test/testfs1"));
        assertEquals(new String(f1.getBytes()), s);
        txn.commit();
        /* System.out.println("------------------------------------------");
         FuseGetattrSetter getattrSetter=new FuseGetattrSetter(){

         @Override
         public void set(long l, int i, int i1, int i2, int i3, int i4, long l1, long l2, int i5, int i6, int i7) {
         // throw new UnsupportedOperationException("Not supported yet.");
         System.out.println("l:"+l+" i:"+i+" i1:"+i1+" i2:"+i2+" i3:"+i3+" i4:"+i4+" l1:"+l1+" i5:"+i5+" i6:"+i6+" i7:"+i7);
         }
                
         };
         FilesystemFuse fs2=new FilesystemFuse();
         FilesystemFuse.server=server;
         assertEquals(0,fs2.getattr("/test/testfs1/file1.txt",  getattrSetter));
         System.out.println("------------------------------------------");*/
        /*} catch (Exception ex) {
            
         }*/
    }
}
