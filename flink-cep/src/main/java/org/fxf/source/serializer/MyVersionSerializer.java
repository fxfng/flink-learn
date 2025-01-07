package org.fxf.source.serializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.fxf.source.split.MySourceSplit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MyVersionSerializer<SplitT extends MySourceSplit<?>> implements SimpleVersionedSerializer<SplitT>, Serializable {
    private static final int serialVersionUID = 1;

    @Override
    public int getVersion() {
        return serialVersionUID;
    }

    @Override
    public byte[] serialize(SplitT split) throws IOException {
        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream out = new ObjectOutputStream(baos)){
            out.writeInt(serialVersionUID);
            out.writeUTF(split.splitId());
            int size = split.getElements().size();
            out.writeInt(size);
            for (Object element : split.getElements()) {
                out.writeUTF((String) element);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public SplitT deserialize(int version, byte[] serialized) throws IOException {
        try(final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            final ObjectInputStream ois = new ObjectInputStream(bais)){
            int versionIn = ois.readInt();
            if (versionIn != serialVersionUID) {
                throw new IOException("Unsupported version: " + versionIn);
            }
            String splitId = ois.readUTF();
            int size = ois.readInt();
            List<String> elements = new ArrayList<>(size);
            for (int i = 0; i < size; i++){
                elements.add(ois.readUTF());
            }
            return (SplitT) new MySourceSplit(splitId, elements);
        }
    }
}
