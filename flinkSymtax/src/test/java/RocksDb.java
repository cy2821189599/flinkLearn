import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author ：kenor
 * @date ：Created in 2020/9/23 21:49
 * @description：
 * @version: 1.0
 */

public class RocksDb {
    static {
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB rocksdb = RocksDB.open(options, "rocksdb");
        byte[] key = "root".getBytes();
        byte[] value = "20".getBytes();
        rocksdb.put(key, value);
        System.out.println("ok");
        System.out.println(new String(key) + ":"+ new String(rocksdb.get(key)));
        rocksdb.close();
        options.close();
    }
}
