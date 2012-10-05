package swift.application.filesystem.fuse;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import fuse.Errno;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
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
    public int flush(String arg0, Object arg1) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int fsync(String arg0, Object arg1, boolean arg2) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getattr(String arg0, FuseGetattrSetter arg1) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getdir(String arg0, FuseDirFiller arg1) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int link(String from, String to) throws FuseException {
        // FIXME Links are future work...
        return Errno.EROFS;
    }

    @Override
    public int mkdir(String arg0, int arg1) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int mknod(String arg0, int arg1, int arg2) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int open(String arg0, int arg1, FuseOpenSetter arg2) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int read(String arg0, Object arg1, ByteBuffer arg2, long arg3) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int readlink(String path, CharBuffer link) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int release(String arg0, Object arg1, int arg2) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int rename(String from, String to) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int rmdir(String path) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int statfs(FuseStatfsSetter arg0) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int symlink(String arg0, String arg1) throws FuseException {
        // FIXME Future work...
        return Errno.EROFS;
    }

    @Override
    public int truncate(String arg0, long arg1) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int unlink(String arg0) throws FuseException {
        // FIXME Future work...
        return Errno.EROFS;
    }

    @Override
    public int utime(String path, int atime, int mtime) throws FuseException {
        // Modification times are not supported yet....
        return 0;
    }

    @Override
    public int write(String arg0, Object arg1, boolean arg2, ByteBuffer arg3, long arg4) throws FuseException {
        // TODO Auto-generated method stub
        return 0;
    }

    public static void main(String[] args) {
        log.info("entering");
        try {
            FuseMount.mount(args, new FilesystemFuse(), log);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("exiting");
        }
    }

}
