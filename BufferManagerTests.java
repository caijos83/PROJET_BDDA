import java.io.*;
import java.nio.ByteBuffer;
import java.lang.Exception;
public class BufferManagerTests {
    public static void main(String[] args) throws IOException {
        DBConfig config = new DBConfig("C:\\Users\\emman\\OneDrive\\Bureau\\L3\\BDDA\\BDD\\DB", 4096, 22288, DBConfig.BMpolicy.LRU, 3);
        DiskManager diskManager = new DiskManager(config);
        BufferManager bufferManager = new BufferManager(config,diskManager);

        testGetPage(bufferManager);
        testMarkPageDirty(bufferManager);
       // testFlushBuffers(bufferManager);
        testReplacementPolicy(bufferManager);
    }


    public static void testGetPage(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        ByteBuffer buffer = bufferManager.getPage(pageId);

        System.out.println("testGetPage passed: " + (buffer != null));
        assert buffer != null : "Failed to load page into buffer!";
    }
    public static void testMarkPageDirty(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        ByteBuffer buffer = bufferManager.getPage(pageId);

        bufferManager.freePage(pageId, true);
        Boolean result = bufferManager.getFlagDirty(pageId);
        System.out.println("testMarkPageDirty passed: " + (result));
        assert result : "Failed to load page into buffer!";
    }
    public static void testFlushBuffers(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);
        bufferManager.getPage(pageId);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);
        bufferManager.freePage(pageId2, true);
        bufferManager.flushBuffers();





    }
    public static void testReplacementPolicy(BufferManager bufferManager) throws IOException {
        PageId pageId = new PageId(0, 1);
        PageId pageId2 = new PageId(0, 2);
        PageId pageId3 = new PageId(0, 3);
        PageId pageId4 = new PageId(0, 4);
        bufferManager.getPage(pageId);
        bufferManager.getPage(pageId2);
        bufferManager.getPage(pageId3);
        bufferManager.getPage(pageId4); // Cette ligne permet d'utiliser les politiques de remplacement
        Boolean r = bufferManager.isLoad(pageId); // affichera False car le nbr de buffer est initialisé à 3 donc on a egalement 3 pages
        System.out.println("testReplacementPolicy passed: " + (!r));
        assert !r : "Failed to load page into buffer!";
    }


}