package miniSGBDR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


public class BufferManager {
    private DBConfig dbConfig;
    private DiskManager diskManager;

    private Map<PageId, ByteBuffer> bufferPool;
    private Map<PageId, Boolean> dirtyMap;
    private Map<PageId, Integer> pinCountMap;


    private LinkedList<PageId> usageOrder;

    public BufferManager(DBConfig dbConfig, DiskManager diskManager) {
        this.dbConfig = dbConfig;
        this.diskManager = diskManager;
        this.bufferPool = new HashMap<>();
        this.dirtyMap = new HashMap<>();
        this.pinCountMap = new HashMap<>();
        this.usageOrder = new LinkedList<>();
    }

    public ByteBuffer getPage(PageId pageId) throws IOException {
        ByteBuffer buffer = bufferPool.get(pageId);
        if (buffer == null) {
            buffer = ByteBuffer.allocate(dbConfig.getPagesize());

            if (bufferPool.size() >= dbConfig.getBm_buffercount()) {
                evictPage();
            }

            diskManager.readPage(pageId, buffer);
            bufferPool.put(pageId, buffer);
            dirtyMap.put(pageId, false);
            pinCountMap.put(pageId, 0);
        }

        pinCountMap.put(pageId, pinCountMap.get(pageId) + 1);

        usageOrder.remove(pageId);
        usageOrder.addLast(pageId);

        return buffer;
    }

    public void freePage(PageId pageId, boolean valdirty) {
        if (pinCountMap.containsKey(pageId)) {
            int current = pinCountMap.get(pageId);
            if (current > 0) {
                pinCountMap.put(pageId, current - 1);
            }
        }
        if (valdirty) {
            dirtyMap.put(pageId, true);
        }
    }

    public void setCurrentReplacementPolicy(String policy) {
        DBConfig.BMpolicy newPolicy;
        try {
            newPolicy = DBConfig.BMpolicy.valueOf(policy);
            this.dbConfig = new DBConfig(dbConfig.getDbpath(), dbConfig.getPagesize(), dbConfig.getDm_maxfilesize(), newPolicy, dbConfig.getBm_buffercount());
        } catch (IllegalArgumentException e) {
            System.err.println("Politique inconnue : " + policy);
        }
    }

    public void flushBuffers() throws IOException {
        for (Map.Entry<PageId, ByteBuffer> entry : bufferPool.entrySet()) {
            PageId pageId = entry.getKey();
            ByteBuffer buf = entry.getValue();
            if (Boolean.TRUE.equals(dirtyMap.get(pageId))) {
                diskManager.writePage(pageId, buf);
            }
        }

        bufferPool.clear();
        dirtyMap.clear();
        pinCountMap.clear();
        usageOrder.clear();
    }

    private void evictPage() throws IOException {
        if (bufferPool.isEmpty()) return;

        PageId pageToEvict;
        if (dbConfig.getBm_policy() == DBConfig.BMpolicy.LRU) {
            pageToEvict = usageOrder.getFirst();
        } else {
            pageToEvict = usageOrder.getLast();
        }


        usageOrder.remove(pageToEvict);

        if (Boolean.TRUE.equals(dirtyMap.get(pageToEvict))) {
            diskManager.writePage(pageToEvict, bufferPool.get(pageToEvict));
        }

        bufferPool.remove(pageToEvict);
        dirtyMap.remove(pageToEvict);
        pinCountMap.remove(pageToEvict);
    }

    public boolean isLoad(PageId pageId) {
        return bufferPool.containsKey(pageId);
    }

    public Boolean getFlagDirty(PageId pageId) throws IOException {
        Boolean isDirty = dirtyMap.get(pageId);
        if (isDirty == null) {
            throw new IOException("La pageId spécifiée n'est pas dans le buffer pool.");
        }
        return isDirty;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public Map<PageId, Boolean> getFlagDirtyMap() {
        return dirtyMap;
    }

    public Map<PageId, ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    public Map<PageId, Integer> getPinCount() {
        return pinCountMap;
    }
}
