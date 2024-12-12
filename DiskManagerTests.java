package tests;

import miniSGBDR.DBConfig;
import miniSGBDR.DiskManager;
import miniSGBDR.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DiskManagerTests {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        DBConfig config = new DBConfig(".\\Data", 4096, 20480, DBConfig.BMpolicy.LRU, 3);
        DiskManager diskManager = new DiskManager(config);

        testAllocPage(diskManager);
        testReadWritePage(diskManager);
        testDeallocPage(diskManager);
        testSaveLoadState(diskManager);

        System.out.println("All DiskManagerTests passed successfully!");
    }

    public static void testAllocPage(DiskManager diskManager) throws IOException {
        PageId pageId = diskManager.allocPage();
        System.out.println("Allocated PageId: " + pageId);
        assert pageId != null : "Allocation failed!";
    }

    public static void testReadWritePage(DiskManager diskManager) throws IOException {
        PageId pageId = diskManager.allocPage();
        int pageSize = 4096;
        ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);
    
        String message = "Hello World!";
        byte[] hello = message.getBytes();
    
        byte[] d = new byte[pageSize];
        // Remplir avec des zéros (déjà fait par défaut)
        // Copier "Hello World!" au début
        System.arraycopy(hello, 0, d, 0, hello.length);
    
        writeBuffer.put(d);
        writeBuffer.flip();
    
        diskManager.writePage(pageId, writeBuffer);
    
        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
        diskManager.readPage(pageId, readBuffer);
    
        byte[] data = new byte[readBuffer.remaining()];
        readBuffer.get(data);
    
        // Extraire exactement le nombre d’octets correspondant au message
        byte[] extracted = new byte[hello.length];
        System.arraycopy(data, 0, extracted, 0, hello.length);
    
        String content = new String(extracted);
        System.out.println("Content Read: " + content);
        assert content.equals("Hello World!") : "Content mismatch!";
    }
    

    public static void testDeallocPage(DiskManager diskManager) {
        try {
            PageId pageId = diskManager.allocPage();
            diskManager.deallocPage(pageId);
            System.out.println("Page deallocated: " + pageId);
        } catch (IOException e) {
            System.err.println("Error in deallocation: " + e.getMessage());
        }
    }


    public static void testSaveLoadState(DiskManager diskManager) throws IOException, ClassNotFoundException {
        diskManager.SaveState();
        diskManager.LoadState();
        System.out.println("DiskManager state saved and loaded successfully.");
    }
    
}
