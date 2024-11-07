/*package projet_SGBD;*/
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.List;

public class TestRelation {
    public static void main(String[] args) throws IOException {
        // Création de la relation
        DBConfig config = DBConfig.loadDBConfig("config.txt");
        DiskManager dm = new DiskManager(config);

        BufferManager bm = new BufferManager(config, dm);

        // Création d'un identifiant de page d'en-tête pour la relation (valeur simulée pour le test)
        PageId headerPageId = new PageId(0, 0);
        Relation relation = new Relation("Students",headerPageId,dm,bm);
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
