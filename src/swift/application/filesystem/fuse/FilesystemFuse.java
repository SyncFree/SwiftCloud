package swift.application.filesystem.fuse;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
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
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.XattrLister;
import fuse.XattrSupport;

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

public class FilesystemFuse implements Filesystem3, XattrSupport {
    private static final Log log = LogFactory.getLog(FilesystemFuse.class);
    private static final String sequencerName = "localhost";
    private static final String scoutName = "localhost";
    private static Swift server;
    private static Filesystem fs;
    private static final int MODE = 0777;
    private static final int BLOCK_SIZE = 512;
    private static final int NAME_LENGTH = 1024;
    private static final String ROOT = "test";

    public static Swift getServer() {
        return server;
    }

    public static void setServer(Swift server) {
        FilesystemFuse.server = server;
    }

    public static Filesystem getFs() {
        return fs;
    }

    public static void setFs(Filesystem fs) {
        FilesystemFuse.fs = fs;
    }

    @Override
    public int chmod(String path, int mode) throws FuseException {
        // No model implemented
        log.info("chmod for " + path);
        return Errno.EROFS;
    }

    @Override
    public int chown(String path, int uid, int gid) throws FuseException {
        // No ownership model implemented
        log.info("chown for " + path);
        return Errno.EROFS;
    }

    @Override
    public int flush(String path, Object fileHandle) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("flush for " + path);
        synchronized (this) {
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
        }
        return Errno.EROFS;
    }

    @Override
    public int fsync(String path, Object fileHandle, boolean isDatasync) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("flush for " + remotePath);
        synchronized (this) {
            TxnHandle txn = null;
            try {
                txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
                IFile f = (IFile) fileHandle;
                File fstub = new File(remotePath);
                fs.updateFile(txn, fstub.getName(), fstub.getParent(), f);
                txn.commit();
                txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
                fileHandle = fs.readFile(txn, fstub.getName(), fstub.getParent());
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
        }
        return Errno.EROFS;
    }

    @Override
    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        String remotePath = getRemotePath(path);
        File fstub = new File(remotePath);
        log.info("getattr for " + remotePath);
        int time = (int) (System.currentTimeMillis() / 1000L);
        synchronized (this) {
            TxnHandle txn = null;
            try {
                txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
                if ("/".equals(path)) {
                    DirectoryTxnLocal root = fs.getDirectory(txn, "/" + ROOT);
                    getattrSetter.set(root.hashCode(), FuseFtypeConstants.TYPE_DIR | MODE, 1, 0, 0, 0, root.getValue()
                            .size() * NAME_LENGTH,
                            (root.getValue().size() * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE, time, time, time);
                } else if (fs.isDirectory(txn, fstub.getName(), fstub.getParent())) {
                    DirectoryTxnLocal dir = fs.getDirectory(txn, remotePath);
                    getattrSetter.set(dir.hashCode(), FuseFtypeConstants.TYPE_DIR | MODE, 1, 0, 0, 0, dir.getValue()
                            .size() * NAME_LENGTH, (dir.getValue().size() * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE,
                            time, time, time);
                } else if (fs.isFile(txn, fstub.getName(), fstub.getParent())) {
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
                e.printStackTrace();
            } catch (VersionNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            txn.rollback();
        }
        return Errno.ENOENT;

    }

    @Override
    public int getdir(String path, FuseDirFiller filler) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("getdir for " + remotePath);
        synchronized (this) {
            TxnHandle txn = null;
            try {
                txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, true);
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
            txn.rollback();
        }
        return Errno.EROFS;
    }

    @Override
    public int link(String from, String to) throws FuseException {
        // FIXME Links are future work...
        log.info("link from " + from + " to " + to);
        return Errno.EROFS;
    }

    @Override
    public int mkdir(String path, int mode) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("mkdir for " + remotePath);
        synchronized (this) {
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
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            txn.rollback();
        }
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
        synchronized (this) {
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
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            txn.rollback();
        }
        return Errno.EROFS;
    }

    @Override
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("open for " + remotePath);
        synchronized (this) {
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
        }
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
        return Errno.EROFS;
    }

    @Override
    public int release(String path, Object fileHandle, int flags) throws FuseException {
        // No action required here
        log.info("release for " + path);
        return 0;
    }

    @Override
    public int rename(String from, String to) throws FuseException {
        // TODO Auto-generated method stub
        log.info("rename from " + from + " to " + to);
        return Errno.EROFS;
    }

    @Override
    public int rmdir(String path) throws FuseException {
        String remotePath = getRemotePath(path);
        log.info("rmdir for " + remotePath);
        synchronized (this) {
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
        }
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
        String remotePath = getRemotePath(path);
        log.info("Unlink for " + remotePath);
        synchronized (this) {
            TxnHandle txn = null;
            try {
                txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);
                File f = new File(remotePath);
                fs.removeFile(txn, f.getName(), f.getParent());
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
        }
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

        if (fh instanceof IFile) {
            IFile f = (IFile) fh;
            f.update(buf, offset);
        }
        return 0;
    }

    //
    // XattrSupport implementation

    /**
     * This method will be called to get the value of the extended attribute
     * 
     * @param path
     *            the path to file or directory containing extended attribute
     * @param name
     *            the name of the extended attribute
     * @param dst
     *            a ByteBuffer that should be filled with the value of the
     *            extended attribute
     * @return 0 if Ok or errno when error
     * @throws fuse.FuseException
     *             an alternative to returning errno is to throw this exception
     *             with errno initialized
     * @throws java.nio.BufferOverflowException
     *             should be thrown to indicate that the given <code>dst</code>
     *             ByteBuffer is not large enough to hold the attribute's value.
     *             After that <code>getxattr()</code> method will be called
     *             again with a larger buffer.
     */
    public int getxattr(String path, String name, ByteBuffer dst, int position) throws FuseException,
            BufferOverflowException {
        log.info("getxattr " + name + " for " + path);
        return Errno.ENOATTR;
    }

    /**
     * This method can be called to query for the size of the extended attribute
     * 
     * @param path
     *            the path to file or directory containing extended attribute
     * @param name
     *            the name of the extended attribute
     * @param sizeSetter
     *            a callback interface that should be used to set the
     *            attribute's size
     * @return 0 if Ok or errno when error
     * @throws fuse.FuseException
     *             an alternative to returning errno is to throw this exception
     *             with errno initialized
     */
    public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
        log.info("getxattrsize " + name + " for " + path);

        return Errno.ENOATTR;
    }

    /**
     * This method will be called to get the list of extended attribute names
     * 
     * @param path
     *            the path to file or directory containing extended attributes
     * @param lister
     *            a callback interface that should be used to list the attribute
     *            names
     * @return 0 if Ok or errno when error
     * @throws fuse.FuseException
     *             an alternative to returning errno is to throw this exception
     *             with errno initialized
     */
    public int listxattr(String path, XattrLister lister) throws FuseException {
        log.info("listxattr for " + path);

        return Errno.ENOATTR;
    }

    /**
     * This method will be called to remove the extended attribute
     * 
     * @param path
     *            the path to file or directory containing extended attributes
     * @param name
     *            the name of the extended attribute
     * @return 0 if Ok or errno when error
     * @throws fuse.FuseException
     *             an alternative to returning errno is to throw this exception
     *             with errno initialized
     */
    public int removexattr(String path, String name) throws FuseException {
        log.info("removexattr " + name + " for " + path);
        return Errno.ENOATTR;
    }

    /**
     * This method will be called to set the value of an extended attribute
     * 
     * @param path
     *            the path to file or directory containing extended attributes
     * @param name
     *            the name of the extended attribute
     * @param value
     *            the value of the extended attribute
     * @param flags
     *            parameter can be used to refine the semantics of the
     *            operation.
     *            <p>
     *            <code>XATTR_CREATE</code> specifies a pure create, which
     *            should fail with <code>Errno.EEXIST</code> if the named
     *            attribute exists already.
     *            <p>
     *            <code>XATTR_REPLACE</code> specifies a pure replace operation,
     *            which should fail with <code>Errno.ENOATTR</code> if the named
     *            attribute does not already exist.
     *            <p>
     *            By default (no flags), the extended attribute will be created
     *            if need be, or will simply replace the value if the attribute
     *            exists.
     * @return 0 if Ok or errno when error
     * @throws fuse.FuseException
     *             an alternative to returning errno is to throw this exception
     *             with errno initialized
     */
    public int setxattr(String path, String name, ByteBuffer value, int flags, int position) throws FuseException {
        log.info("setxattr " + name + " for " + path);

        return Errno.ENOATTR;
    }

    public static void main(String[] args) {
        log.info("setting up servers");
        initServerInfrastructure();

        try {
            log.info("getting root directory");
            TxnHandle txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

            // create a root directory
            // FIXME make this part of arguments
            fs = new FilesystemBasic(txn, ROOT, "DIR");
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
