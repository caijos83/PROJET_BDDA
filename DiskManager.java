package miniSGBDR;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DiskManager {
    private final DBConfig config;
    private List<PageId> freePages;

    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();

        File binDataDir = new File(config.getDbpath() + "/BinData");
        if (!binDataDir.exists()) {
            binDataDir.mkdirs();
        }
    }


    public PageId allocPage() throws IOException {
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        int fileIdx = 0;
        File binDataDir = new File(config.getDbpath() + "/BinData");
        File[] files = binDataDir.listFiles((dir, name) -> name.startsWith("F") && name.endsWith(".rsdb"));

        if (files != null) {
            boolean pageAllocated = false;
            int pageIdx = 0;

            for (File f : files) {
                String fname = f.getName(); 
                int idx = Integer.parseInt(fname.substring(1, fname.indexOf(".rsdb")));
                long currentSize = f.length();
                long maxSize = config.getDm_maxfilesize();
                if (currentSize + config.getPagesize() <= maxSize) {
                    fileIdx = idx;
                    pageIdx = (int)(currentSize / config.getPagesize());
                    pageAllocated = true;
                    break;
                }
            }

            if (!pageAllocated) {
                fileIdx = files.length;
                pageIdx = 0;
            }

            PageId pid = new PageId(fileIdx, pageIdx);
            ensurePageExists(pid);
            return pid;
        } else {
            fileIdx = 0;
            int pageIdx = 0;
            PageId pid = new PageId(fileIdx, pageIdx);
            ensurePageExists(pid);
            return pid;
        }
    }

    private void ensurePageExists(PageId pid) throws IOException {
        File file = new File(config.getDbpath() + "/BinData/F" + pid.getFileIdx() + ".rsdb");
        if (!file.exists()) {
            file.createNewFile();
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long requiredSize = (long)(pid.getPageIdx() + 1) * config.getPagesize();
            if (raf.length() < requiredSize) {
                raf.setLength(requiredSize);
            }
        }
    }

    public void readPage(PageId pageId, java.nio.ByteBuffer buffer) throws IOException {
        File file = new File(config.getDbpath() + "/BinData/F" + pageId.getFileIdx() + ".rsdb");

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long position = (long) pageId.getPageIdx() * config.getPagesize();                
            raf.seek(position);
            byte[] data = new byte[config.getPagesize()];
            raf.readFully(data);
            buffer.clear();
            buffer.put(data);
            buffer.flip();
        }
    }

    public void writePage(PageId pageId, java.nio.ByteBuffer buffer) throws IOException {
        File file = new File(config.getDbpath() + "/BinData/F" + pageId.getFileIdx() + ".rsdb");
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long position = (long) pageId.getPageIdx() * config.getPagesize();
            raf.seek(position);
            byte[] data = new byte[config.getPagesize()];
            buffer.rewind();
            buffer.get(data);
            raf.write(data);
        }
    }

    public void deallocPage(PageId pageId) {
        if (!freePages.contains(pageId)) {
            freePages.add(pageId);
        }
    }

    public void SaveState() throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(config.getDbpath() + "/BinData/dm.save"))) {
            out.writeObject(freePages);
        }
    }

    @SuppressWarnings("unchecked")
    public void LoadState() throws IOException, ClassNotFoundException {
        File saveFile = new File(config.getDbpath() + "/BinData/dm.save");
        if (!saveFile.exists()) {
            System.out.println("No saved state found.");
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(saveFile))) {
            freePages = (List<PageId>) in.readObject();
        }
    }
}
