package swift.application.filesystem.cs;

import static sys.net.api.Networking.Networking;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import swift.application.filesystem.cs.proto.ChmodOperation;
import swift.application.filesystem.cs.proto.ChownOperation;
import swift.application.filesystem.cs.proto.FSyncOperation;
import swift.application.filesystem.cs.proto.FlushOperation;
import swift.application.filesystem.cs.proto.FuseOperationResult;
import swift.application.filesystem.cs.proto.FuseRemoteOperation;
import swift.application.filesystem.cs.proto.FuseResultHandler;
import swift.application.filesystem.cs.proto.GetAttrOperation;
import swift.application.filesystem.cs.proto.GetDirOperation;
import swift.application.filesystem.cs.proto.LinkOperation;
import swift.application.filesystem.cs.proto.MkdirOperation;
import swift.application.filesystem.cs.proto.MknodOperation;
import swift.application.filesystem.cs.proto.OpenOperation;
import swift.application.filesystem.cs.proto.ReadLinkOperation;
import swift.application.filesystem.cs.proto.ReadOperation;
import swift.application.filesystem.cs.proto.ReleaseOperation;
import swift.application.filesystem.cs.proto.RenameOperation;
import swift.application.filesystem.cs.proto.RmdirOperation;
import swift.application.filesystem.cs.proto.SymLinkOperation;
import swift.application.filesystem.cs.proto.TruncateOperation;
import swift.application.filesystem.cs.proto.UTimeOperation;
import swift.application.filesystem.cs.proto.UnlinkOperation;
import swift.application.filesystem.cs.proto.WriteOperation;
import sys.Sys;
import sys.net.api.Endpoint;
import sys.net.api.Networking.TransportProvider;
import sys.net.api.rpc.RpcEndpoint;
import sys.net.api.rpc.RpcHandle;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.XattrLister;
import fuse.XattrSupport;

public class SwiftFuseClient implements Filesystem3, XattrSupport {
    public static final int PORT = 10001;

    private static final Log log = LogFactory.getLog(SwiftFuseClient.class);

    Endpoint server;
    RpcEndpoint endpoint;

    /** Cached attributes */
    private final Map<String, GetAttrOperation.Result> attributes;
    /** Expiration dates */
    private final Map<String, Long> expirationDateAttr;
    private final long defaultExpirationAttr;
    private final ExecutorService threads;

    SwiftFuseClient(long expFile) {
        this.attributes = Collections.synchronizedMap(new HashMap<String, GetAttrOperation.Result>());
        this.expirationDateAttr = Collections.synchronizedMap(new HashMap<String, Long>());
        this.defaultExpirationAttr = expFile;
        this.threads = Executors.newFixedThreadPool(128);
        Executors.newScheduledThreadPool(2).scheduleWithFixedDelay(this.removeExpired(),
                this.defaultExpirationAttr / 2, this.defaultExpirationAttr, TimeUnit.SECONDS);
    }

    private Runnable removeExpired() {
        return new Runnable() {
            public void run() {
                for (final Entry<String, Long> e : expirationDateAttr.entrySet()) {
                    if (System.currentTimeMillis() > e.getValue()) {
                        threads.execute(removeExpiredEntry(e.getKey()));
                    }
                }
            }
        };

    }

    Runnable removeExpiredEntry(final String path) {
        return new Runnable() {
            public void run() {
                attributes.remove(path);
                expirationDateAttr.remove(path);
            }
        };
    }

    void init(String[] args) {

        // SwiftFuseServer.main( new String[] {} );

        try {
            if (args.length < 3) {
                System.out.println("Usage: [fuse scout/server address] [fuse args]");
                return;
            } else {
                Sys.init();

                log.info("Initializing the system");

                String fuseServer = args[0];
                server = Networking.resolve(fuseServer, SwiftFuseServer.PORT);
                endpoint = Networking.rpcConnect(TransportProvider.DEFAULT).toDefaultService();

                log.info("mounting filesystem");

                String[] fuse_args = new String[args.length - 1];
                System.arraycopy(args, 1, fuse_args, 0, fuse_args.length);
                FuseMount.mount(fuse_args, this, log);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("exiting");
        }
    }

    public static void main(String[] args) {
        new SwiftFuseClient(3000).init(args);
    }

    @Override
    public int chmod(String path, int mode) throws FuseException {
        return send1(new ChmodOperation(path, mode)).intResult();
    }

    @Override
    public int chown(String path, int uid, int gid) throws FuseException {
        return send1(new ChownOperation(path, uid, gid)).intResult();
    }

    @Override
    public int flush(String path, Object handle) throws FuseException {
        this.threads.execute(removeExpiredEntry(path));
        return send1(new FlushOperation(path, handle)).intResult();
    }

    @Override
    public int fsync(String path, Object fileHandle, boolean isDatasync) throws FuseException {
        this.threads.execute(removeExpiredEntry(path));
        return send1(new FSyncOperation(path, fileHandle, isDatasync)).intResult();
    }

    // file attr valid for 3 secs (NFS)
    // dir attr valid for 30-60 secs (NFS)
    // For consistency, attr cache must be invalidated if corresponding item is
    // flushed or fsync'ed

    // WISHME always caching special attr e.g. for home dir
    @Override
    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        GetAttrOperation.Result res = null;
        Long expireTime = this.expirationDateAttr.get(path);
        if (expireTime != null) {
            if (System.currentTimeMillis() > expireTime) {
                this.threads.execute(removeExpiredEntry(path));
            } else {
                res = this.attributes.get(path);
            }
        } else {
            res = send2(new GetAttrOperation(path, getattrSetter));
            // add to cache
            this.attributes.put(path, res);
            this.expirationDateAttr.put(path, System.currentTimeMillis() + this.defaultExpirationAttr);
        }
        // prepare the setter and return result code
        res.applyTo(getattrSetter);
        return res.intResult();
    }

    @Override
    public int getdir(String path, FuseDirFiller filler) throws FuseException {
        GetDirOperation.Result res = send2(new GetDirOperation(path));
        res.applyTo(filler);
        return res.intResult();
    }

    @Override
    public int link(String from, String to) throws FuseException {
        return send1(new LinkOperation(from, to)).intResult();
    }

    @Override
    public int mkdir(String path, int mode) throws FuseException {
        return send1(new MkdirOperation(path, mode)).intResult();
    }

    @Override
    public int mknod(String path, int mode, int rdev) throws FuseException {
        return send1(new MknodOperation(path, mode, rdev)).intResult();
    }

    @Override
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        OpenOperation.Result res = send2(new OpenOperation(path, flags, openSetter));
        res.applyTo(openSetter);
        return res.intResult();
    }

    @Override
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
        ReadOperation.Result res = send2(new ReadOperation(path, fh, buf, offset));
        res.applyTo(buf);
        return res.intResult();
    }

    @Override
    public int readlink(String path, CharBuffer link) throws FuseException {
        ReadLinkOperation.Result res = send2(new ReadLinkOperation(path, link));
        res.applyTo(link);
        return res.intResult();
    }

    @Override
    public int release(String path, Object fileHandle, int flags) throws FuseException {
        return send1(new ReleaseOperation(path, fileHandle, flags)).intResult();
    }

    @Override
    public int rename(String from, String to) throws FuseException {
        return send1(new RenameOperation(from, to)).intResult();
    }

    @Override
    public int rmdir(String path) throws FuseException {
        return send1(new RmdirOperation(path)).intResult();
    }

    @Override
    public int statfs(FuseStatfsSetter setter) throws FuseException {
        // return send( new StatFsOperation( setter)).intResult() ;
        return 0;
    }

    @Override
    public int symlink(String from, String to) throws FuseException {
        return send1(new SymLinkOperation(from, to)).intResult();
    }

    @Override
    public int truncate(String path, long mode) throws FuseException {
        return send1(new TruncateOperation(path, mode)).intResult();
    }

    @Override
    public int unlink(String path) throws FuseException {
        return send1(new UnlinkOperation(path)).intResult();
    }

    @Override
    public int utime(String path, int atime, int mtime) throws FuseException {
        return send1(new UTimeOperation(path, atime, mtime)).intResult();
    }

    @Override
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        return send1(new WriteOperation(path, fh, isWritepage, buf, offset)).intResult();
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
        return 0;
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

        return 0;
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

        return 0;
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
        return 0;
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

        return 0;
    }

    private FuseOperationResult send1(FuseRemoteOperation op) throws FuseException {
        RpcHandle result = endpoint.send(server, op, retHandler);
        if (!result.failed()) {
            return (FuseOperationResult) (result.getReply().getPayload());
        } else {
            throw new FuseException("Failed to Talk to the Fuse Server");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T send2(FuseRemoteOperation op) throws FuseException {
        RpcHandle result = endpoint.send(server, op, retHandler);
        if (!result.failed()) {
            return (T) (result.getReply().getPayload());
        } else {
            throw new FuseException("Failed to Talk to the Fuse Server");
        }
    }

    static class ResultHandler extends FuseResultHandler {
        public void onReceive(FuseOperationResult m) {
        }
    }

    static final ResultHandler retHandler = new ResultHandler();

}
