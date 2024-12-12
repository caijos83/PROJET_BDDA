package tests;

import miniSGBDR.*;
import miniSGBDR.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RelationTests {
    public static void main(String[] args) throws Exception {
        DBConfig dbConfig = new DBConfig(".\\Data", 4096, 20480,
                DBConfig.BMpolicy.LRU, 5);
        DiskManager diskManager = new DiskManager(dbConfig);
        BufferManager bufferManager = new BufferManager(dbConfig, diskManager);
        PageId headerPage = diskManager.allocPage();

        List<String> colNames = new ArrayList<>();
        colNames.add("ID");
        colNames.add("Name");
        colNames.add("Age");

        List<String> colTypes = new ArrayList<>();
        colTypes.add("INT");
        colTypes.add("VARCHAR(10)");
        colTypes.add("INT");

        Relation relation = new Relation("TestRelation", colNames, colTypes, headerPage, diskManager, bufferManager, dbConfig);

        testGetDataPages(relation);
        testWriteRecordToBuffer(relation);
        testReadFromBuffer(relation);
        testAddDataPage(relation, bufferManager, headerPage);
        testGetFreeDataPageId(relation, bufferManager, headerPage);
        testWriteRecordToDataPage(relation);
        testGetRecordsInDataPage(relation, bufferManager);
        testInsertRecord(relation);

        System.out.println("All RelationTests passed successfully!");
    }

    public static void testWriteRecordToBuffer(Relation relation) {
        miniSGBDR.Record record = new miniSGBDR.Record();
        record.addValue(1);       
        record.addValue("Alice"); 
        record.addValue(30);      

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 0;

        int writtenSize = relation.writeRecordToBuffer(record, buffer, pos);
        System.out.println("testWriteRecordToBuffer: Record written successfully, size = " + writtenSize);
        assert writtenSize > 0 : "Failed to write record to buffer!";
    }

    public static void testReadFromBuffer(Relation relation) {
        miniSGBDR.Record record = new miniSGBDR.Record();
        record.addValue(1);       
        record.addValue("Alice"); 
        record.addValue(30);      

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int pos = 0;
        relation.writeRecordToBuffer(record, buffer, pos);

        miniSGBDR.Record readRecord = new miniSGBDR.Record();
        int readSize = relation.readFromBuffer(readRecord, buffer, pos);
        System.out.println("testReadFromBuffer: Record read successfully, size = " + readSize);
        System.out.println("Read Record: " + readRecord.getValues());

        assert readSize > 0 : "Failed to read record from buffer!";
        assert readRecord.getValues().equals(record.getValues()) : "Mismatch in record values!";
    }

    public static void testAddDataPage(Relation relation, BufferManager bufferManager, PageId headerPage) throws IOException {
        ByteBuffer headerBuffer = bufferManager.getPage(headerPage);
        headerBuffer.position(0);
        int numPages = headerBuffer.getInt();

        relation.addDataPage();

        bufferManager.freePage(headerPage, false);
        headerBuffer = bufferManager.getPage(headerPage);
        headerBuffer.position(0);
        int newNumPages = headerBuffer.getInt();

        System.out.println("testAddDataPage: AddDataPage successfully, numPages = " + newNumPages);
        assert newNumPages == numPages + 1 : "Test AddDataPage Failed!";
        bufferManager.freePage(headerPage, false);
    }

    public static void testGetFreeDataPageId(Relation relation, BufferManager bufferManager, PageId headerPage) throws IOException {
        PageId freepage = relation.getFreeDataPageId(100);
        assert freepage != null : "Test GetFreeDataPageId Failed: returned null page!";

        ByteBuffer headerBuffer = bufferManager.getPage(headerPage);
        headerBuffer.position(0);
        int pageCount = headerBuffer.getInt();

        boolean found = false;
        for (int i = 0; i < pageCount; i++) {
            int offset = 4 + i * 12;
            headerBuffer.position(offset);
            int fileIdx = headerBuffer.getInt();
            int pageIndex = headerBuffer.getInt();
            int freeSpace = headerBuffer.getInt();

            if (fileIdx == freepage.getFileIdx() && pageIndex == freepage.getPageIdx()) {
                found = true;
                System.out.println("testGetFreeDataPageId: freeSpace = " + freeSpace);
                assert freeSpace >= 100 : "GetFreeDataPageId: not enough free space!";
                break;
            }
        }

        assert found : "GetFreeDataPageId: page not found in header!";
        bufferManager.freePage(headerPage, false);
    }

    public static void testWriteRecordToDataPage(Relation relation) throws IOException {
        miniSGBDR.Record record = new miniSGBDR.Record();
        record.addValue(1);       
        record.addValue("Alice"); 
        record.addValue(25);      

        relation.addDataPage();

        PageId pageId = relation.getFreeDataPageId(100);
        RecordId recordId = relation.writeRecordToDataPage(record, pageId);

        System.out.println("testWriteRecordToDataPage passed: " +
                (recordId != null && recordId.getPageId().equals(pageId) && recordId.getSlotIdx() == 0));

        assert recordId != null : "Failed to write record to data page!";
        assert recordId.getPageId().equals(pageId) : "PageId mismatch in RecordId!";
        assert recordId.getSlotIdx() == 0 : "Slot index mismatch in RecordId!";
    }

    public static void testGetRecordsInDataPage(Relation relation, BufferManager bufferManager) throws IOException {
        ByteBuffer headerBuf = bufferManager.getPage(relation.getHeaderPageId());
        headerBuf.position(0);
        headerBuf.putInt(0); 
        bufferManager.freePage(relation.getHeaderPageId(), true);
        bufferManager.flushBuffers();

        miniSGBDR.Record record1 = new miniSGBDR.Record();
        record1.addValue(1);
        record1.addValue("Alice");
        record1.addValue(25);

        miniSGBDR.Record record2 = new miniSGBDR.Record();
        record2.addValue(2);
        record2.addValue("Bob");
        record2.addValue(30);

        relation.addDataPage();
        PageId pageId = relation.getFreeDataPageId(200);

        relation.writeRecordToDataPage(record1, pageId);
        relation.writeRecordToDataPage(record2, pageId);

        bufferManager.flushBuffers();

        List<miniSGBDR.Record> records = relation.getRecordsInDataPage(pageId);

        boolean testPassed = (records.size() == 2 &&
                            records.get(0).getValues().equals(record1.getValues()) &&
                            records.get(1).getValues().equals(record2.getValues()));

        System.out.println("testGetRecordsInDataPage passed: " + testPassed + " size=" + records.size());

        assert records.size() == 2 : "Incorrect number of records read from data page!";
        assert records.get(0).getValues().equals(record1.getValues()) : "First record mismatch!";
        assert records.get(1).getValues().equals(record2.getValues()) : "Second record mismatch!";
    }


    public static void testGetDataPages(Relation relation) throws IOException {
        for (int i = 0; i < 3; i++) {
            relation.addDataPage();
        }

        List<PageId> pages = relation.getDataPages();
        assert pages.size() == 3 : "Incorrect number of Data Pages returned!";

        boolean pageNotNull = true;
        for (PageId pageId : pages) {
            if (pageId == null) {
                pageNotNull = false;
                break;
            }
        }
        assert pageNotNull : "Null PageId encountered in Data Pages list!";

        System.out.println("testGetDataPages passed : " + ((pages.size() == 3) && pageNotNull));
    }

    public static void testInsertRecord(Relation relation) throws IOException {
        miniSGBDR.Record record = new Record();
        record.addValue(1);
        record.addValue("Alice");
        record.addValue(25);

        RecordId recordId = relation.InsertRecord(record);
        boolean test = (recordId != null);
        System.out.println("testInsertRecord passed : " + test);
        assert test : "Failed to insert record!";
    }
}
