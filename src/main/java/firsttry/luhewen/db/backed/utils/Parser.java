package firsttry.luhewen.db.backed.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static byte[] long2Byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
