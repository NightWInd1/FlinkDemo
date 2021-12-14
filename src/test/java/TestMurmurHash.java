import org.apache.flink.shaded.netty4.io.netty.util.internal.MathUtil;
import org.apache.flink.util.MathUtils;

public class TestMurmurHash {
    public static void main(String[] args) {
        int a =0;
        int b =1;

        String c ="偶数";
        String d = "奇数";

        int a_murmur = MathUtils.murmurHash(0);
        int b_murmur = MathUtils.murmurHash(1);
        int c_murmur = MathUtils.murmurHash(c.hashCode());
        int d_murmur = MathUtils.murmurHash(d.hashCode());

        System.out.println(a_murmur%128);
        System.out.println(b_murmur%128);
        System.out.println(c_murmur%128);
        System.out.println(d_murmur%128);


    }
}
