package tests;

import miniSGBDR.BufferManager;
import miniSGBDR.DBConfig;
import miniSGBDR.DiskManager;
import miniSGBDR.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferManagerTests {
    public static void main(String[] args) throws IOException {
        DBConfig config = new DBConfig(".\\Data", 4096, 22288, DBConfig.BMpolicy.LRU, 3);
        DiskManager diskManager = new DiskManager(config);
        BufferManager bufferManager = new BufferManager(config, diskManager);

        testGetPage(bufferManager);
        testMarkPageDirty(bufferManager);
        testFlushBuffers(bufferManager);
        testReplacementPolicy(bufferManager);

        System.out.println("All BufferManagerTests passed successfully!");
    }

    public static void testGetPage(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        ByteBuffer buffer = bufferManager.getPage(pageId);
        System.out.println("testGetPage passed: " + (buffer != null));
        assert buffer != null : "Failed to load page into buffer.";
    }

    public static void testMarkPageDirty(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        bufferManager.getPage(pageId);
        bufferManager.freePage(pageId, true); 
        Boolean result = bufferManager.getFlagDirty(pageId);
        System.out.println("testMarkPageDirty passed: " + result);
        assert result : "Failed to mark page as modified.";
    }

    public static void testFlushBuffers(BufferManager bufferManager) throws IOException {
        DiskManager diskManager = bufferManager.getDiskManager();
        diskManager.allocPage();
        diskManager.allocPage();
        diskManager.allocPage();

        PageId pageId1 = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);

        bufferManager.getPage(pageId1);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);

        bufferManager.freePage(pageId2, true);

        bufferManager.flushBuffers();

        boolean cond = bufferManager.getPinCount().isEmpty() &&
                       bufferManager.getFlagDirtyMap().isEmpty() &&
                       bufferManager.getBufferPool().isEmpty();

        System.out.println("testFlushBuffers passed: " + cond);
        assert cond : "Flush buffers did not clear all structures.";
    }

    public static void testReplacementPolicy(BufferManager bufferManager) throws IOException {
        DiskManager dm = bufferManager.getDiskManager();
        dm.allocPage();
        dm.allocPage();
        dm.allocPage();
        dm.allocPage();

        PageId pageId = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);
        PageId pageId4 = new PageId(0, 4);

        bufferManager.getPage(pageId);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);
        bufferManager.getPage(pageId4); 

        Boolean isLoaded = bufferManager.isLoad(pageId); 
        System.out.println("testReplacementPolicy passed: " + (!isLoaded));
        assert !isLoaded : "Replacement policy did not evict the expected page.";
    }
}
