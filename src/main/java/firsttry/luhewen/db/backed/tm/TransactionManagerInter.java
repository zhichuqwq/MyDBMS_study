package firsttry.luhewen.db.backed.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import firsttry.luhewen.db.commom.Error;
import firsttry.luhewen.db.backed.utils.Panic;
import firsttry.luhewen.db.backed.utils.Parser;

public class TransactionManagerInter implements TransactionManager{
    static final int LEN_XID_HEADER_LENGTH = 8;   //XID文件头长度
    private static final int XID_FIELD_SIZE = 1;  //每个事务占用长度

    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIEID_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    //超级事务，无论何时都是committed状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFiX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerInter(RandomAccessFile raf, FileChannel fc){
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
    }

    /**
     * 检查XID文件是否合法
     */
    private void checkXIDCounter(){
        long filelen = 0;
        try{
            filelen = file.length();
        }catch(IOException e){
            Panic.panic(e);
        }
        if(filelen < LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try{
            fc.position(0);
            fc.read(buf);
        }catch(IOException e){
            Panic.panic(e);
        }
        this.xidCounter = buf.getLong();
        if(getXidPosition(xidCounter) != filelen){
            Panic.panic(Error.BadLogFileException);
        }
    }

    //根据事务xid获得在xid文件中的位置
    private long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid + 1) * XID_FIELD_SIZE;
    }

    //更新事务的状态
    private void updateXID(long xid, byte status){
        Long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        buf.put(status);
        try{
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try{
            fc.force(false);
        }catch(IOException e){
            Panic.panic(e);
        }
    }

    //将xid加一，更新xid header
    private  void incrXIDCounter(){
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try{
            fc.position(0);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    //开始事务，返回xid
    public long begin(){
        counterLock.lock();
        try{
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    //提交事务
    public void commit(long xid){
        updateXID(xid, FIEID_TRAN_COMMITTED);
    }

    //回滚xid事务
    public void abort(long xid){updateXID(xid, FIELD_TRAN_ABORTED);}

    //检测xid事务是否处于status状态
    private  boolean checkXID(long xid, byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    //判断事务是否active
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    //判断事务是否提交
    public boolean isCommitted(long xid){
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIEID_TRAN_COMMITTED);
    }

    //判断事务是否回滚
    public boolean isAborted(long xid){
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close(){
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}

