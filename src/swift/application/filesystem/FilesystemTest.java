package swift.application.filesystem;

import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.crdt.RegisterVersioned;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class FilesystemTest {
    private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("swift.filesystem");

    public static void main(String[] args) {
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }

        DCServer.main(new String[] { sequencerName });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Sys.init();
        Swift server = SwiftImpl.newInstance(scoutName, DCConstants.SURROGATE_PORT);

        TxnHandle txn;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

            // create a root directory
            logger.info("Creating file system");
            // Filesystem fs = new FilesystemBasic(txn, "test", "DIR",
            // RegisterFileContent.class);
            Filesystem fs = new FilesystemBasic(txn, "test", "DIR", RegisterVersioned.class);
            txn.commit();

            logger.info("Creating directories and subdirectories");
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            fs.createDirectory(txn, "testfs1", "/test");
            fs.createDirectory(txn, "testfs2", "/test");

            fs.createDirectory(txn, "include", "/test/testfs1");
            fs.createDirectory(txn, "sys", "/test/testfs1/include");
            fs.createDirectory(txn, "netinet", "/test/testfs1/include");

            txn.commit();

            logger.info("Creating a file");
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            IFile f1 = fs.createFile(txn, "file1.txt", "/test/testfs1");
            String s = "This is a test file";
            f1.update(s, 0);
            fs.updateFile(txn, "file1.txt", "/test/testfs1", f1);
            txn.commit();

            logger.info("Reading from the file");
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            IFile f1_up = fs.readFile(txn, "file1.txt", "/test/testfs1");
            assert (f1_up.getContent().equals(s));

            logger.info("Updating the file");
            String prefix = "Yes! ";
            f1_up.update(prefix, 0);
            fs.updateFile(txn, "file1.txt", "/test/testfs1", f1_up);
            assert (f1_up.getContent().equals(prefix + s));
            txn.commit();

            logger.info("Checking that updates are committed");
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            IFile f1_upp = fs.readFile(txn, "file1.txt", "/test/testfs1");
            assert (f1_upp.getContent().equals(prefix + s));
            txn.commit();

            logger.info("Copying the file");
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            fs.copyFile(txn, "file1.txt", "/test/testfs1", "/test/testfs2");
            IFile f1_copy = fs.readFile(txn, "file1.txt", "/test/testfs2");
            assert (f1_copy.getContent().equals(prefix + s));
            txn.commit();

            // mkdir testfs1 testfs1/include testfs1/include/sys
            // testfs1/include/netinet
            // mkdir testfs2 testfs2/include testfs2/include/sys
            // testfs2/include/netinet
            // mkdir testfs3 testfs3/include testfs3/include/sys
            // testfs3/include/netinet
            // mkdir testfs4 testfs4/include testfs4/include/sys
            // testfs4/include/netinet
            // mkdir testfs5 testfs5/include testfs5/include/sys
            // testfs5/include/netinet

            // Phase II: Copying files
            // cp $(ORIGINAL)/fscript/DrawString.c testfs1/DrawString.c
            // etc.

            // Phase III: Recursive directory stats *********"
            // find . -print -exec ls -l {} \;
            // du -s *

            // # Exercises proportional to length of file
            // @echo "********* Phase IV: Scanning each file *********"
            // find . -exec grep kangaroo {} \;
            // find . -exec wc {} \;

        } catch (Exception e) {
            e.printStackTrace();
        }
        server.stop(true);
    }
}
