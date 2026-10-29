// Reproduce the ObjectOutputStream HashMap<byte[], byte[]> format without Kafka.
// Compile to a temporary directory. Output is a synthetic fixture, never real state.
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class OffsetFixture {
    public static void main(String[] args) throws Exception {
        var key = args[0].getBytes(StandardCharsets.UTF_8);
        var value = args[1].getBytes(StandardCharsets.UTF_8);
        var map = new HashMap<Object,Object>();
        map.put(key, value);
        switch (args[2]) {
            case "duplicate": map.put(key.clone(), value.clone()); break;
            case "shared": map.put(key, key); break;
            case "null": map.put(key, null); break;
            case "string": map.put(key, "not a byte array"); break;
            case "object": map.put(key, new Date(0)); break;
        }
        var bytes = new ByteArrayOutputStream();
        try (var output = new ObjectOutputStream(bytes)) { output.writeObject(map); }
        System.out.println(Base64.getEncoder().encodeToString(bytes.toByteArray()));
    }
}
