package sys.net.impl;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class Lz4Lib {

    private static final ThreadLocal<LZ4Compressor> lz4Compressor = new ThreadLocal<LZ4Compressor>() {
        LZ4Factory factory = LZ4Factory.fastestInstance();

        @Override
        protected LZ4Compressor initialValue() {
            return factory.highCompressor();
        }
    };

    public static LZ4Compressor lz4Compressor() {
        return lz4Compressor.get();
    }

    private static final ThreadLocal<LZ4SafeDecompressor> lz4Decompressor = new ThreadLocal<LZ4SafeDecompressor>() {
        LZ4Factory factory = LZ4Factory.fastestInstance();

        @Override
        protected LZ4SafeDecompressor initialValue() {
            return factory.safeDecompressor();
        }
    };

    public static LZ4SafeDecompressor lz4Decompressor() {
        return lz4Decompressor.get();
    }

}
