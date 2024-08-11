package firsttry.luhewen.db.backed.tm;

//TM 通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import firsttry.luhewen.db.backed.utils.Panic;
import firsttry.luhewen.db.commom.Error;

public interface TransactionManager {
    long begin();                      //开启新事务
    void commit(long xid);             //提交一个事物
    void abort(long xid);              //取消一个事务
    boolean isActive(long xid);        //查询一个事务的否处于进行的状态
    boolean isCommitted(long xid);     //查询一个事务是否已经提交
    boolean isAborted(long xid);       //查询一个事务是否已经取消
    void close();                      //关闭TM

    //新建一下XID文件并创建TM
    public static TransactionManagerInter create(String path){
        File f = new File(path + TransactionManagerInter.XID_SUFFiX);
        try{
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch(Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch(FileNotFoundException e){
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.allocate(TransactionManagerInter.LEN_XID_HEADER_LENGTH);
        try{
            fc.position(0);
            fc.write(buf);
        }catch(IOException e){
            Panic.panic(e);
        }

        return new TransactionManagerInter(raf, fc);
    }

    //通过已经存在的XID文件创建TM
    public static TransactionManagerInter open(String path){
        File f = new File(path + TransactionManagerInter.XID_SUFFiX);
        if(!f.exists()){
            Panic.panic(Error.FileExistsException);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try{
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch(FileNotFoundException e){
            Panic.panic(e);
        }

        return new TransactionManagerInter(raf, fc);
    }
}
