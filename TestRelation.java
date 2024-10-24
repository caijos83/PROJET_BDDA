/*package projet_SGBD;*/
import java.nio.ByteBuffer;

public class TestRelation {
    public static void main(String[] args) {
        // Création de la relation
        Relation relation = new Relation("Students");
        relation.addColumn("id", "INT");
        relation.addColumn("gpa", "REAL");
        relation.addColumn("name", "VARCHAR(50)");

        // Création du record
        Record record = new Record();
        record.addValue(12);
        record.addValue(3.5f);
        record.addValue("John Does");

        // Ecriture dans le buffer
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Allocation d'un buffer de 1024 octets
        int bytesWritten = relation.writeRecordToBuffer(record, buffer, 0);
        System.out.println("Bytes written: " + bytesWritten);

        // Lecture depuis le buffer
        Record readRecord = new Record();
        int bytesRead = relation.readFromBuffer(readRecord, buffer, 0);
        System.out.println("Bytes read: " + bytesRead);

        // Affichage des valeurs
        System.out.println("Read Record:");
        for (Object value : readRecord.getValues()) {
            System.out.println(value);
        }
    }
}
