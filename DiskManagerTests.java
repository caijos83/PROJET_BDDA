

import java.io.IOException;
import java.nio.ByteBuffer;

public class DiskManagerTests {

    public static void main(String[] args) throws IOException, ClassNotFoundException  {
        DBConfig config = new DBConfig("./Data", 4096, 1048576, DBConfig.BMpolicy.LRU, 5);
        DiskManager diskManager = new DiskManager(config);

        testAllocPage(diskManager);
        testReadWritePage(diskManager);
        testDeallocPage(diskManager);
        testSaveLoadState(diskManager);
    }

    public static void testAllocPage(DiskManager diskManager) throws IOException {

        PageId pageId = diskManager.allocPage("TestTable");
        System.out.println("Allocated PageId: " + pageId);
        assert pageId != null : "Allocation failed!";
    }

    public static void testReadWritePage(DiskManager diskManager) throws IOException {
        PageId pageId = diskManager.allocPage("TestTable");
        ByteBuffer writeBuffer = ByteBuffer.allocate(5096);
        writeBuffer.put("Hello World!".getBytes());


        // Write the buffer to the page
        diskManager.writePage(pageId, writeBuffer);

        // Read the page back into a buffer
        ByteBuffer readBuffer = ByteBuffer.allocate(5096);
        diskManager.readPage(pageId, readBuffer);

        String content = new String(readBuffer.array()).trim();
        System.out.println("Content Read: " + content);
        assert content.equals("Hello World!") : "Content mismatch!";
    }

    public static void testDeallocPage(DiskManager diskManager) {
        try {
            PageId pageId = diskManager.allocPage("TestTable");
            diskManager.deallocPage(pageId);
            System.out.println("Page deallocated: " + pageId);
        } catch (IOException e) {
            System.err.println("Error in deallocation: " + e.getMessage());
        }
    }


    public static void testSaveLoadState(DiskManager diskManager) throws IOException, ClassNotFoundException {
        diskManager.saveState();
        diskManager.loadState();
        System.out.println("DiskManager state saved and loaded successfully.");
    }

}
