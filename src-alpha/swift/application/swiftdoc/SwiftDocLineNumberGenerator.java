package swift.application.swiftdoc;


public class SwiftDocLineNumberGenerator<V> {

    public void parseFiles(SwiftDocOps<V> seq) throws Exception {

        if (seq != null)
            seq.begin();

        for (int i = 0; i < 1000000; i++)
            if (seq != null) {
                seq.add(seq.size(), seq.gen(String.format("%10d", i)));
            }

        if (seq != null)
            seq.commit();
    }
}
