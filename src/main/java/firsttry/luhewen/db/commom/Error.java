package firsttry.luhewen.db.commom;

public class Error {
    public static final Exception CacheFullException = new RuntimeException("Cache full !");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotFoundException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");

    public static final Exception FileNotExistsException = new RuntimeException("File not exists!");

}
