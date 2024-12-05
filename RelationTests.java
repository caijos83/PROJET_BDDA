package miniSGBDR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RelationTests {
	public static void main(String[] args) throws Exception {
		DBConfig dbConfig = new DBConfig(".\\Data", 4096, 1048576,
				DBConfig.BMpolicy.LRU, 5);
		DiskManager diskManager = new DiskManager(dbConfig);
		BufferManager bufferManager = new BufferManager(dbConfig, diskManager);
		PageId headerPage = new PageId(0,0);
		Relation relation = new Relation("TestRelation", headerPage, diskManager, bufferManager);
		relation.addColumn("ID", "INT");
		relation.addColumn("Name", "VARCHAR(10)");
		relation.addColumn("Age", "INT");

		testGetDataPages(relation);

		testWriteRecordToBuffer(relation);
		testReadFromBuffer(relation);
		testAddDataPage(relation);
		testGetFreeDataPageId(relation);
		testWriteRecordToDataPage(relation);
		testGetRecordsInDataPage(relation);


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

	public static void testAddDataPage(Relation relation) throws IOException {


			ByteBuffer headerBuffer = relation.getBufferManager().getPage(relation.getHeaderPageId());
			int numPages = headerBuffer.getInt(0);
			relation.addDataPage();
			if( headerBuffer.getInt(0) == numPages + 1) {
				System.out.println("testAddDataPage: AddDataPage successfully, numPages = " + headerBuffer.getInt(0));
			}
			assert headerBuffer.getInt(0) == numPages + 1 : "Test AddDataPage Faild !";


	}

	public static void testGetFreeDataPageId(Relation relation) throws IOException {
		PageId freepage = relation.getFreeDataPageId(100);
		if (freepage != null) {
			ByteBuffer headerBuffer = relation.getBufferManager().getPage(relation.getHeaderPageId());
			int pageCount = headerBuffer.getInt(0);

			for (int i = 0; i < pageCount; i++) {
				int offset = 4 + i * 12;
				int fileIdx = headerBuffer.getInt(offset);
				int pageIndex = headerBuffer.getInt(offset + 4);
				int freeSpace = headerBuffer.getInt(offset + 8);

				if (fileIdx == freepage.getFileIdx() && pageIndex == freepage.getPageIdx()) {
					if (freeSpace >= 100) {
						System.out.println(
								"testGetFreeDataPageId: GetFreeDataPageId successfully, freeSpace = " + freeSpace);
						assert freeSpace >= 100 : "Test GetFreeDataPageId Faild !";
						break;
					}
				}

			}

		} else {
			assert freepage != null : "Test GetFreeDataPageId Faild !";
		}

	}
	

	public static void testWriteRecordToDataPage(Relation relation) throws IOException {
		// Créer un record à écrire
		Record record = new Record();
		record.addValue(1); // ID
		record.addValue("Alice"); // Name
		record.addValue(25); // Age
	
		// Ajouter une nouvelle page de données à la relation
		relation.addDataPage();
	
		// Récupérer la première page disponible
		PageId pageId = relation.getFreeDataPageId(100);
	
		// Écrire le record dans la page
		RecordId recordId = relation.writeRecordToDataPage(record, pageId);
	

		System.out.println("testWriteRecordToDataPage passed: " + (recordId != null  &&  recordId.getPageId().equals(pageId) && recordId.getSlotIdx() == 0));

		// Vérifier que le RecordId est valide
		assert recordId != null : "Failed to write record to data page!";
		assert recordId.getPageId().equals(pageId) : "PageId mismatch in RecordId!";
		assert recordId.getSlotIdx() == 0 : "Slot index mismatch in RecordId!";
	
	}

	
	public static void testGetRecordsInDataPage(Relation relation) throws IOException {
    // Créer des records à écrire
    Record record1 = new Record();
    record1.addValue(1); // ID
    record1.addValue("Alice"); // Name
    record1.addValue(25); // Age

    Record record2 = new Record();
    record2.addValue(2); // ID
    record2.addValue("Bob"); // Name
    record2.addValue(30); // Age

    // Ajouter une nouvelle page de données à la relation
    relation.addDataPage();

    // Récupérer la première page disponible
    PageId pageId = relation.getFreeDataPageId(200);

    // Écrire les records dans la page
    relation.writeRecordToDataPage(record1, pageId);
    relation.writeRecordToDataPage(record2, pageId);

    // Lire tous les records de la page
    List<Record> records = relation.getRecordsInDataPage(pageId);


		System.out.println("testGetRecordsInDataPage passed: " + ((records.size() == 2)  &&  (records.get(0).getValues().equals(record1.getValues())) && (records.get(1).getValues().equals(record2.getValues()))));
	
    // Vérifier le nombre de records
    assert records.size() == 2 : "Incorrect number of records read from data page!";

    // Vérifier le contenu des records
    assert records.get(0).getValues().equals(record1.getValues()) : "First record mismatch!";
    assert records.get(1).getValues().equals(record2.getValues()) : "Second record mismatch!";
	}

	public static void testGetDataPages(Relation relation) throws IOException {

		// Ajouter 3 pages de données à la relation
		for (int i = 0; i < 3; i++) {
			relation.addDataPage();
		}

		// Récupérer la liste des PageIds
		List<PageId> pages = relation.getDataPages();

		// Vérifier que la liste contient exactement 3 pages
		System.out.println(pages.size() );
		assert pages.size() == 3 : "Incorrect number of Data Pages returned!";

		boolean pageNotNull = true;
		for (PageId pageId : pages) {
			if (pageId ==  null){
				pageNotNull = false;
				break;
			}
			assert pageNotNull==true : "Null PageId encountered in Data Pages list!";
        }

		System.out.println("testGetDataPages passed : " + ( (pages.size() == 3)&&(pageNotNull) ) );
	}





}
