
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class DiskManagerTests {
    public static void main(String[] args) throws IOException, ClassNotFoundException  {
        DBConfig config = new DBConfig("C:\\Users\\manon\\OneDrive\\Documents\\L3\\Data", 4096, 12288, DBConfig.BMpolicy.LRU, 3);
        DiskManager diskManager = new DiskManager(config);

        testAllocPage(diskManager);
        testReadWritePage(diskManager);
        testDeallocPage(diskManager);    
        testSaveLoadState(diskManager);
    }

    // Test allocating a page
    public static void testAllocPage(DiskManager diskManager) throws IOException {
        PageId pageId = diskManager.allocPage();
        System.out.println("Allocated PageId: " + pageId);
        assert pageId != null : "Allocation failed!";
    }

    // Test reading and writing a page
    public static void testReadWritePage(DiskManager diskManager) throws IOException {
        PageId pageId = diskManager.allocPage();
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

    // Test deallocating a page
    public static void testDeallocPage(DiskManager diskManager) {
        try {
            PageId pageId = diskManager.allocPage();
            diskManager.deallocPage(pageId);
            System.out.println("Page deallocated: " + pageId);
        } catch (IOException e) {
            System.err.println("Error in deallocation: " + e.getMessage());
        }
    }

    // Test saving and loading state
 // Test saving and loading state
    public static void testSaveLoadState(DiskManager diskManager) throws IOException, ClassNotFoundException {
        diskManager.saveState();
        diskManager.loadState();
        System.out.println("DiskManager state saved and loaded successfully.");
    }

}