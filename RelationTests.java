

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RelationTests {
    public static void main(String[] args) throws Exception {
        Relation relation = new Relation("TestRelation");
        relation.addColumn("ID", "INT");
        relation.addColumn("Name", "VARCHAR(10)");
        relation.addColumn("Age", "INT");

        testWriteRecordToBuffer(relation);
        testReadFromBuffer(relation);
    }

    public static void testWriteRecordToBuffer(Relation relation) {
        Record record = new Record();
        record.addValue(1); // ID
        record.addValue("Alice"); // Name
        record.addValue(30); // Age

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 0;

        int writtenSize = relation.writeRecordToBuffer(record, buffer, pos);
        System.out.println("testWriteRecordToBuffer: Record written successfully, size = " + writtenSize);
        assert writtenSize > 0 : "Failed to write record to buffer!";
    }

    public static void testReadFromBuffer(Relation relation) {
        Record record = new Record();
        record.addValue(1); // ID
        record.addValue("Alice"); // Name
        record.addValue(30); // Age

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 0;
        relation.writeRecordToBuffer(record, buffer, pos);

        Record readRecord = new Record();
        int readSize = relation.readFromBuffer(readRecord, buffer, pos);
        System.out.println("testReadFromBuffer: Record read successfully, size = " + readSize);
        System.out.println("Read Record: " + readRecord.getValues());

        assert readSize > 0 : "Failed to read record from buffer!";
        assert readRecord.getValues().equals(record.getValues()) : "Mismatch in record values!";
    }
}
