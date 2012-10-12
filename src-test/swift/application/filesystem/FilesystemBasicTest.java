package swift.application.filesystem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import swift.client.SwiftImpl;
import swift.crdt.DirectoryTxnLocal;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.Swift;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.utils.Pair;
import sys.Sys;

/**
 * 
 * @author annettebieniusa
 */
public class FilesystemBasicTest {

    private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("swift.filesystem");
    private static Swift server;
    private static TxnHandle txn;
    private static Filesystem fs;

    @BeforeClass
    public static void onlyOnce() throws NetworkException, WrongTypeException, NoSuchObjectException,
            VersionNotFoundException {
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
        server = SwiftImpl.newInstance(scoutName, DCConstants.SURROGATE_PORT);

        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
        fs = new FilesystemBasic(txn, "test", "DIR");
        txn.commit();
    }

    @Before
    public void initializeTxn() throws NetworkException {
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
    }

    @After
    public void commitTxn() throws NetworkException {
        txn.commit();
    }

    @Test
    public void testCreatingDirectories() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        fs.createDirectory(txn, "testfs2", "/test");
        assertTrue(fs.isDirectory(txn, "testfs2", "/test"));

        DirectoryTxnLocal dir = fs.getDirectory(txn, "/test");
        assertTrue(dir.getValue().contains(new Pair<String, Class<?>>("testfs2", DirectoryVersioned.class)));
    }

    @Test
    public void testSubDirectories() throws WrongTypeException, NoSuchObjectException, VersionNotFoundException,
            NetworkException, ClassNotFoundException {
        fs.createDirectory(txn, "testfs1", "/test");
        assertTrue(fs.isDirectory(txn, "testfs1", "/test"));

        fs.createDirectory(txn, "include", "/test/testfs1");
        fs.createDirectory(txn, "sys", "/test/testfs1/include");
        fs.createDirectory(txn, "netinet", "/test/testfs1/include");
        assertTrue(fs.isDirectory(txn, "include", "/test/testfs1"));
        assertTrue(fs.isDirectory(txn, "sys", "/test/testfs1/include"));
        assertTrue(fs.isDirectory(txn, "netinet", "/test/testfs1/include"));

        DirectoryTxnLocal dir = fs.getDirectory(txn, "/test/testfs1/include");
        assertTrue(dir.getValue().contains(new Pair<String, Class<?>>("sys", DirectoryVersioned.class)));
        assertTrue(dir.getValue().contains(new Pair<String, Class<?>>("netinet", DirectoryVersioned.class)));
    }

    @Test
    public void testCreatingSourceFile() throws Exception {
        IFile f1 = fs.createFile(txn, "file1.txt", "/test");
        assertTrue(fs.isFile(txn, "file1.txt", "/test"));
        assertTrue(Arrays.equals(f1.getBytes(), new byte[0]));
    }

    @Test
    public void testCreatingBlobFile() throws Exception {
        IFile f1 = fs.createFile(txn, "file2.blob", "/test");
        assertTrue(Arrays.equals(f1.getBytes(), new byte[0]));

    }

    @Test
    public void testResetBlobFile() throws Exception {
        IFile f1 = fs.createFile(txn, "file3.blob", "/test");
        String s = "This is a blob test file";
        f1.reset(s.getBytes());
        assertTrue(Arrays.equals(f1.getBytes(), s.getBytes()));
    }

    @Test
    public void testResetSourceFile() throws Exception {
        IFile f1 = fs.createFile(txn, "file4.txt", "/test");
        String s = "This is a source test file";
        f1.reset(s.getBytes());
        assertTrue(Arrays.equals(f1.getBytes(), s.getBytes()));
    }

    @Test
    public void testReadFile() throws Exception {
        String fname = "file5.txt";
        String path = "/test";
        IFile f1 = fs.createFile(txn, fname, path);
        String s = "This is a source test file";
        f1.reset(s.getBytes());
        fs.updateFile(txn, fname, path, f1);
        commitTxn();

        // Reading from the file
        initializeTxn();
        IFile f1_up = fs.readFile(txn, fname, path);
        assert (Arrays.equals(f1_up.getBytes(), s.getBytes()));
    }

    @Test
    public void testUpdateBlobFile() throws Exception {
        IFile f1 = fs.createFile(txn, "fileUp.blob", "/test");
        String s = "This is a blob test file";
        f1.reset(s.getBytes());

        String prefix = "Yes! ";
        byte[] concat = (prefix + s).getBytes();

        ByteBuffer buf_up = ByteBuffer.wrap(concat);
        f1.update(buf_up, 0);
        assert (Arrays.equals(f1.getBytes(), concat));
    }

    @Test
    public void testUpdateSourceFile() throws Exception {
        IFile f1 = fs.createFile(txn, "fileUp.txt", "/test");
        String s = "This is a source test file";
        f1.reset(s.getBytes());

        String prefix = "Yes! ";
        byte[] concat = (prefix + s).getBytes();

        ByteBuffer buf_up = ByteBuffer.wrap(concat);
        f1.update(buf_up, 0);
        assert (Arrays.equals(f1.getBytes(), concat));
    }

    @Test
    public void testCopyFile() throws Exception {
        String orig = "cp1";
        String dest = "cp2";
        String path = "/test";
        String origpath = path + "/" + orig;
        String destpath = path + "/" + dest;

        String fname = "fileCopyTest.txt";
        fs.createDirectory(txn, orig, path);
        fs.createDirectory(txn, dest, path);

        IFile f1 = fs.createFile(txn, fname, origpath);
        String s = "This is a source test file";
        f1.reset(s.getBytes());
        fs.updateFile(txn, fname, origpath, f1);

        fs.copyFile(txn, fname, origpath, destpath);
        assertTrue(fs.isFile(txn, fname, origpath));
        assertTrue(fs.isFile(txn, fname, destpath));
        IFile f1_copy = fs.readFile(txn, fname, destpath);
        assertTrue(Arrays.equals(f1_copy.getBytes(), s.getBytes()));
    }

    @Test
    public void testRemove() throws Exception {
        String fname = "fileRemoveTest.txt";
        String orig = "orig";
        String path = "/test";
        String origpath = path + "/" + orig;

        fs.createDirectory(txn, orig, path);
        fs.createFile(txn, fname, origpath);
        assertTrue(fs.isFile(txn, fname, origpath));
        fs.removeFile(txn, fname, origpath);
        assertFalse(fs.isFile(txn, fname, origpath));
    }

    @Test
    public void testRecreate() throws Exception {
        String fname = "fileRemoveTest.txt";
        String orig = "orig";
        String path = "/test";
        String origpath = path + "/" + orig;

        fs.createDirectory(txn, orig, path);
        IFile f = fs.createFile(txn, fname, origpath);
        String s = "This is a test file for recreating";
        f.reset(s.getBytes());
        fs.updateFile(txn, fname, origpath, f);
        commitTxn();

        initializeTxn();
        fs.removeFile(txn, fname, origpath);
        commitTxn();

        initializeTxn();
        fs.createFile(txn, fname, origpath);
        assertTrue(fs.isFile(txn, fname, origpath));

        IFile f1 = fs.readFile(txn, fname, origpath);
        assertTrue(Arrays.equals(f1.getBytes(), new byte[0]));
    }
}
