package swift.application.filesystem.fuse;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import swift.application.filesystem.Filesystem;
import swift.application.filesystem.FilesystemBasic;
import swift.application.filesystem.IFile;
import swift.client.SwiftImpl;
import swift.crdt.DirectoryTxnLocal;
import swift.crdt.DirectoryVersioned;
import swift.crdt.RegisterVersioned;
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
import fuse.Errno;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseFtypeConstants;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseStatfsSetter;

/**
 * FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi
 * (mszeredi@inf.bme.hu))
 * 
 * The interface description of Fuse says:
 * 
 * The file system operations:
 * 
 * Most of these should work very similarly to the well known UNIX file system
 * operations. Exceptions are:
 * 
 * - All operations should return the error value (errno) by either: - throwing
 * a fuse.FuseException with a errno field of the exception set to the desired
 * fuse.Errno.E* value. - returning an integer value taken from fuse.Errno.E*
 * constants. this is supposed to be less expensive in terms of CPU cycles and
 * should only be used for very frequent errors (for example ENOENT).
 * 
 * - getdir() is the opendir(), readdir(), ..., closedir() sequence in one call.
 * 
 * - There is no create() operation, mknod() will be called for creation of all
 * non directory, non symlink nodes.
 * 
 * - open() No creation, or trunctation flags (O_CREAT, O_EXCL, O_TRUNC) will be
 * passed to open(). Open should only check if the operation is permitted for
 * the given flags.
 * 
 * - read(), write(), release() are are passed a filehandle that is returned
 * from open() in addition to a pathname. The offset of the read and write is
 * passed as the last argument, the number of bytes read/writen is returned
 * through the java.nio.ByteBuffer object
 * 
 * - release() is called when an open file has: 1) all file descriptors closed
 * 2) all memory mappings unmapped This call need only be implemented if this
 * information is required.
 * 
 * - flush() called when a file is closed (can be called multiple times for each
 * dup-ed filehandle)
 * 
 * - fsync() called when file data should be synced (with a flag to sync only
 * data but not metadata)
 * 
 */

public class FilesystemFuse implements Filesystem3 {
    private static final Log log = LogFactory.getLog(FilesystemFuse.class);
    private static final String sequencerName = "localhost";
    private static final String scoutName = "localhost";
    private static Swift server;
    private static Filesystem fs;
    private static final int MODE = 0777;
    private static final int BLOCK_SIZE = 512;
    private static final int NAME_LENGTH = 1024;
    private static final String ROOT = "test";

    @Override
    public int chmod(String path, int mode) throws FuseException {
        // No ownership model implemented
        return 0;
    }

    @Override
    public int chown(String path, int uid, int gid) throws FuseException {
        // No ownership model implemented
        return 0;
    }

    @Override
    public int flush(String path, Object fileHandle) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("flush for " + path);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            IFile f = (IFile) fileHandle;
            File fstub = new File(remotePath);
            fs.updateFile(txn, fstub.getName(), fstub.getParent(), f);
            txn.commit();
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        txn.rollback();
        return Errno.EROFS;
    }

    @Override
    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        // TODO Auto-generated method stub
        log.info("fsync for " + path);

        return 0;
    }

    @Override
    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        String remotePath = getRemotePath(path);
        File fstub = new File(remotePath);

        log.info("getattr for " + remotePath);
        int time = (int) (System.currentTimeMillis() / 1000L);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
            if ("/".equals(path)) {
                DirectoryTxnLocal root = fs.getDirectory(txn, "/" + ROOT);
                getattrSetter.set(root.hashCode(), FuseFtypeConstants.TYPE_DIR | MODE, 1, 0, 0, 0, root.getValue()
                        .size() * NAME_LENGTH, (root.getValue().size() * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE,
                        time, time, time);

            } else if (fs.isDirectory(txn, fstub.getName(), fstub.getParent())) {
                DirectoryTxnLocal dir = fs.getDirectory(txn, remotePath);
                getattrSetter.set(dir.hashCode(), FuseFtypeConstants.TYPE_DIR | MODE, 1, 0, 0, 0, dir.getValue().size()
                        * NAME_LENGTH, (dir.getValue().size() * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE, time, time,
                        time);
            } else if (fs.isFile(txn, fstub.getName(), fstub.getParent())) {
                // FIXME We want files with variable size...
                IFile f = fs.readFile(txn, fstub.getName(), fstub.getParent());
                getattrSetter.set(fstub.hashCode(), FuseFtypeConstants.TYPE_FILE | MODE, 1, 0, 0, 0, f.getSize(),
                        (f.getSize() + BLOCK_SIZE - 1) / BLOCK_SIZE, time, time, time);
            } else {
                txn.rollback();
                return Errno.ENOENT;
            }
            txn.commit();
            return 0;

        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            txn.rollback();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Errno.ENOENT;

    }

    @Override
    public int getdir(String path, FuseDirFiller filler) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("getdir for " + remotePath);
        try {
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
            DirectoryTxnLocal dir = fs.getDirectory(txn, remotePath);
            Collection<Pair<String, Class<?>>> c = dir.getValue();
            for (Pair<String, Class<?>> entry : c) {
                String name = entry.getFirst();
                // TODO This needs to be adapted for links and permissions
                int mode = MODE;
                int ftype = FuseFtypeConstants.TYPE_FILE;
                if (entry.getSecond().equals(DirectoryVersioned.class)) {
                    ftype = FuseFtypeConstants.TYPE_DIR;
                }
                filler.add(name, entry.hashCode(), ftype | mode);
            }
            txn.commit();
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return Errno.EROFS;
    }

    @Override
    public int link(String from, String to) throws FuseException {
        // FIXME Links are future work...
        return Errno.EROFS;
    }

    @Override
    public int mkdir(String path, int mode) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("mkdir for " + remotePath);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            File f = new File(remotePath);
            log.info("creating dir " + f.getName() + " in parentdir " + f.getParent());
            fs.createDirectory(txn, f.getName(), f.getParent());
            txn.commit();
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        txn.rollback();
        return Errno.EROFS;
    }

    private String getRemotePath(String path) {
        if ("/".equals(path)) {
            return "/" + ROOT;
        }
        return "/" + ROOT + path;
    }

    @Override
    public int mknod(String path, int mode, int rdev) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("mknod for " + remotePath);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            File f = new File(remotePath);
            log.info("creating file " + f.getName() + " in parentdir " + f.getParent());
            fs.createFile(txn, f.getName(), f.getParent());
            txn.commit();
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        txn.rollback();
        return Errno.EROFS;
    }

    @Override
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("open for " + remotePath);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
            File fstub = new File(remotePath);
            if (fstub.isDirectory()) {
                txn.rollback();
                throw new FuseException().initErrno(FuseException.EISDIR);
            }

            log.info("opening file " + fstub.getName() + " in parentdir " + fstub.getParent());
            IFile f = fs.readFile(txn, fstub.getName(), fstub.getParent());
            txn.commit();
            openSetter.setFh(f);
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        txn.rollback();
        return Errno.EROFS;
    }

    @Override
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("read for " + remotePath);
        if (fh instanceof IFile) {
            IFile f = (IFile) fh;
            f.read(buf, offset);
            return 0;
        }
        return Errno.EBADF;
    }

    @Override
    public int readlink(String path, CharBuffer link) throws FuseException {
        // TODO Auto-generated method stub
        log.info("readlink for " + path);

        return 0;
    }

    @Override
    public int release(String path, Object fileHandle, int flags) throws FuseException {
        // TODO Auto-generated method stub
        log.info("release for " + path);

        return 0;
    }

    @Override
    public int rename(String from, String to) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int rmdir(String path) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("rmdir for " + remotePath);
        TxnHandle txn = null;
        try {
            txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
            File f = new File(remotePath);
            fs.removeDirectory(txn, f.getName(), f.getParent());
            txn.commit();
            return 0;
        } catch (NetworkException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (WrongTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (VersionNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        txn.rollback();
        return Errno.EROFS;
    }

    @Override
    public int statfs(FuseStatfsSetter arg0) throws FuseException {
        // TODO Auto-generated method stub
        log.info("statfs called");

        return 0;
    }

    @Override
    public int symlink(String from, String to) throws FuseException {
        // FIXME Future work...
        log.info("symlink from " + from + " to " + to);
        return Errno.EROFS;
    }

    @Override
    public int truncate(String path, long mode) throws FuseException {
        // TODO Auto-generated method stub
        log.info("truncate for " + path);
        return 0;
    }

    @Override
    public int unlink(String path) throws FuseException {
        // FIXME Future work...
        log.info("Unlink for " + path);
        return Errno.EROFS;
    }

    @Override
    public int utime(String path, int atime, int mtime) throws FuseException {
        // Modification times are not supported yet....
        log.info("Utime for " + path);
        return 0;
    }

    @Override
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {

        String remotePath = getRemotePath(path);
        log.info("write for " + remotePath);
        // int remaining = buf.remaining();
        // byte[] arr = new byte[remaining];
        // buf.get(arr);

        if (fh instanceof IFile) {
            IFile f = (IFile) fh;
            f.update(buf, offset);
        }
        return 0;
    }

    public static void main(String[] args) {
        log.info("setting up servers");
        initServerInfrastructure();

        try {
            log.info("getting root directory");
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

            // create a root directory
            // FIXME make this part of arguments
            fs = new FilesystemBasic(txn, ROOT, "DIR", RegisterVersioned.class);
            txn.commit();

            log.info("mounting filesystem");
            FuseMount.mount(args, new FilesystemFuse(), log);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("exiting");
        }
    }

    private static void initServerInfrastructure() {
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }
        DCServer.main(new String[] { sequencerName });
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }
        Sys.init();
        server = SwiftImpl.newInstance(scoutName, DCConstants.SURROGATE_PORT);

    }

}
