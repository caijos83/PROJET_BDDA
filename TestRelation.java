/*package projet_SGBD;*/
import java.io.IOException;
import java.nio.ByteBuffer;

public class TestRelation {
    public static void main(String[] args) throws IOException {
        // Création de la relation
    	
    	DBConfig config = DBConfig.loadDBConfig("./config.txt");
        DiskManager dm = new DiskManager(config);
     
        BufferManager bufferManager = new BufferManager(config,dm);
        PageId headerPageId = new PageId(0, 0);
        
        //relation 1 
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
        
        
        
        
        //relation 2
        Relation relation2 = new Relation("Students");
        relation2.addColumn("id", "INT");
        relation2.addColumn("gpa", "REAL");
        relation2.addColumn("name", "VARCHAR(50)");

        // Création du record
        Record record2 = new Record();
        record2.addValue(12);
        record2.addValue(3.5f);
        record2.addValue("John Does");

        // Ecriture dans le buffer
        ByteBuffer buffer2 = ByteBuffer.allocate(1024); // Allocation d'un buffer de 1024 octets
        int bytesWritten2 = relation2.writeRecordToBuffer(record2, buffer2, 0);
        System.out.println("Bytes written: " + bytesWritten2);

        // Lecture depuis le buffer
        Record readRecord2 = new Record();
        int bytesRead2 = relation2.readFromBuffer(readRecord2, buffer2, 0);
        System.out.println("Bytes read: " + bytesRead2);

        // Affichage des valeurs
        System.out.println("Read Record:");
        for (Object value2 : readRecord2.getValues()) {
            System.out.println(value2);
        }
        
        System.out.println("Header Page ID: " + relation2.getHeaderPageId());
    }
}
