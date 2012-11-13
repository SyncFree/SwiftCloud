package swift.application.filesystem.cs.proto;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.introspect.BasicClassIntrospector.GetterMethodFilter;

import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import swift.application.filesystem.cs.proto.GetDirOperation._FuseDirFiller;
import swift.application.filesystem.cs.proto.GetDirOperation._FuseDirFillerItem;
import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;

public class GetAttrOperation extends FuseRemoteOperation {

    String path;

    GetAttrOperation() {
    }

    public GetAttrOperation(String path, FuseGetattrSetter getattrSetter) {
        this.path = path;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        try {
            _FuseGetattrSetter getter = new _FuseGetattrSetter();
            int res = ((RemoteFuseOperationHandler) handler).getattr(path, getter);
            handle.reply(new Result(res, getter));
        } catch (FuseException e) {
            handle.reply(new FuseOperationResult());
        }
    }

    
    public static class Result extends FuseOperationResult {

        _FuseGetattrSetter getter_ret;

        Result() {
        }

        public Result(int ret, _FuseGetattrSetter getter) {
            super(ret);
            this.getter_ret = getter;
        }

        public void applyTo(FuseGetattrSetter getter) {
            for (_GetAttrSetterItem i : getter_ret.items)
                getter.set(i.inode, i.mode, i.nlink, i.uid, i.gid, i.rdev, i.size, i.blocks, i.atime, i.mtime, i.ctime);
        }
        
        public String toString() {
            return String.format("%s (%s, %s)", getClass(), result, getter_ret.items);
        }
    }

    static class _FuseGetattrSetter implements FuseGetattrSetter {

        List<_GetAttrSetterItem> items = new ArrayList<_GetAttrSetterItem>();

        public void set(long inode, int mode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime,
                int mtime, int ctime) {
            items.add(new _GetAttrSetterItem(inode, mode, nlink, uid, gid, rdev, size, blocks, atime, mtime, ctime));
        }
    }

    static class _GetAttrSetterItem {

        long inode;
        int mode;
        int nlink;
        int uid;
        int gid;
        int rdev;
        long size;
        long blocks;
        int atime;
        int mtime;
        int ctime;

        // for kryo
        _GetAttrSetterItem() {
        }

        public _GetAttrSetterItem(long inode, int mode, int nlink, int uid, int gid, int rdev, long size, long blocks,
                int atime, int mtime, int ctime) {
            this.inode = inode;
            this.mode = mode;
            this.nlink = nlink;
            this.uid = uid;
            this.gid = gid;
            this.rdev = rdev;
            this.size = size;
            this.blocks = blocks;
            this.atime = atime;
            this.mtime = mtime;
            this.ctime = ctime;
        }

        public String toString() {
            return String.format("%s %s %s %s %s %s %s %s %s %s", inode, mode, nlink, uid, gid, rdev, size, blocks, atime, mtime, ctime);
        }
    }

}
