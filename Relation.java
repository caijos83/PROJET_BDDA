package miniSGBDR;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation implements Serializable {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes; 

    private transient DiskManager diskManager;
    private transient BufferManager bufferManager;
    private transient DBConfig config;
    private transient PageId headerPageId;

    public Relation(String name, List<String> columnNames, List<String> columnTypes,
                    PageId headerPageId, DiskManager dm, BufferManager bm, DBConfig config) {
        this.name = name;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.headerPageId = headerPageId;
        this.diskManager = dm;
        this.bufferManager = bm;
        this.config = config;
    }

    private int getDataPageCount(ByteBuffer headerBuf) {
        headerBuf.position(0);
        return headerBuf.getInt();
    }

    private void setDataPageCount(ByteBuffer headerBuf, int count) {
        headerBuf.position(0);
        headerBuf.putInt(count);
    }

    private void readDataPageEntry(ByteBuffer headerBuf, int i, int[] entry) {
        int baseOffset = 4 + i * 12;
        headerBuf.position(baseOffset);
        entry[0] = headerBuf.getInt();
        entry[1] = headerBuf.getInt();
        entry[2] = headerBuf.getInt();
    }

    private void writeDataPageEntry(ByteBuffer headerBuf, int i, int fileIdx, int pageIdx, int freeSpace) {
        int baseOffset = 4 + i * 12;
        headerBuf.position(baseOffset);
        headerBuf.putInt(fileIdx);
        headerBuf.putInt(pageIdx);
        headerBuf.putInt(freeSpace);
    }

    public void addDataPage() throws IOException {
        PageId newPage = diskManager.allocPage();
        ByteBuffer pageBuf = bufferManager.getPage(newPage);
        pageBuf.clear();
        pageBuf.putInt(newPage.getFileIdx());
        pageBuf.putInt(newPage.getPageIdx());


        int freeStart = 8;
        int M = 0;

        pageBuf.position(config.getPagesize() - 8);
        pageBuf.putInt(M);          
        pageBuf.putInt(freeStart);  
        bufferManager.freePage(newPage, true);

        ByteBuffer headerBuf = bufferManager.getPage(headerPageId);
        int n = getDataPageCount(headerBuf);
        int freeSpace = config.getPagesize() - 8 - 8;
        writeDataPageEntry(headerBuf, n, newPage.getFileIdx(), newPage.getPageIdx(), freeSpace);
        setDataPageCount(headerBuf, n + 1);
        bufferManager.freePage(headerPageId, true);
    }

    public PageId getFreeDataPageId(int sizeRecord) throws IOException {
        ByteBuffer headerBuf = bufferManager.getPage(headerPageId);
        int n = getDataPageCount(headerBuf);
        for (int i = 0; i < n; i++) {
            int[] entry = new int[3];
            readDataPageEntry(headerBuf, i, entry);
            int freeSpace = entry[2];
            if (freeSpace >= sizeRecord + 8) {
                bufferManager.freePage(headerPageId, false);
                return new PageId(entry[0], entry[1]);
            }
        }
        bufferManager.freePage(headerPageId, false);
        return null;
    }

    public RecordId writeRecordToDataPage(Record record, PageId pageId) throws IOException {
        ByteBuffer pageBuf = bufferManager.getPage(pageId);

        int pageSize = config.getPagesize();
        pageBuf.position(pageSize - 8);
        int M = pageBuf.getInt();
        int freeStart = pageBuf.getInt();

        int recordSize = getRecordSize(record);

        int bytesWritten = writeRecordToBuffer(record, pageBuf, freeStart);
        if (bytesWritten != recordSize) {
            throw new IOException("Record size mismatch.");
        }

        int newSlotIdx = M;
        M++;
        int slotPos = pageSize - 8 - M * 8;
        pageBuf.position(slotPos);
        pageBuf.putInt(freeStart);
        pageBuf.putInt(recordSize);

        pageBuf.position(pageSize - 8);
        pageBuf.putInt(M);
        pageBuf.putInt(freeStart + recordSize);

        int slotDirStart = pageSize - 8 - M * 8;
        int newFreeSpace = slotDirStart - (freeStart + recordSize);

        bufferManager.freePage(pageId, true);

        ByteBuffer headerBuf = bufferManager.getPage(headerPageId);
        int n = getDataPageCount(headerBuf);
        for (int i = 0; i < n; i++) {
            int[] entry = new int[3];
            readDataPageEntry(headerBuf, i, entry);
            if (entry[0] == pageId.getFileIdx() && entry[1] == pageId.getPageIdx()) {
                writeDataPageEntry(headerBuf, i, entry[0], entry[1], newFreeSpace);
                break;
            }
        }
        bufferManager.freePage(headerPageId, true);

        return new RecordId(pageId, newSlotIdx);
    }

    public List<Record> getRecordsInDataPage(PageId pageId) throws IOException {
        List<Record> records = new ArrayList<>();
        ByteBuffer pageBuf = bufferManager.getPage(pageId);
        int pageSize = config.getPagesize();

        pageBuf.position(pageSize - 8);
        int M = pageBuf.getInt();
        pageBuf.getInt(); 

        for (int slotIdx = 0; slotIdx < M; slotIdx++) {
            int slotPos = pageSize - 8 - (slotIdx + 1)*8;
            pageBuf.position(slotPos);
            int startPos = pageBuf.getInt();
            int length = pageBuf.getInt();
            if (length > 0) {
                Record r = new Record();
                readFromBuffer(r, pageBuf, startPos);
                records.add(r);
            }
        }

        bufferManager.freePage(pageId, false);
        return records;
    }

    public List<PageId> getDataPages() throws IOException {
        List<PageId> pages = new ArrayList<>();
        ByteBuffer headerBuf = bufferManager.getPage(headerPageId);
        int n = getDataPageCount(headerBuf);
        for (int i = 0; i < n; i++) {
            int[] entry = new int[3];
            readDataPageEntry(headerBuf, i, entry);
            pages.add(new PageId(entry[0], entry[1]));
        }
        bufferManager.freePage(headerPageId, false);
        return pages;
    }

    public RecordId InsertRecord(Record record) throws IOException {
        int size = getRecordSize(record);
        PageId p = getFreeDataPageId(size);
        if (p == null) {
            addDataPage();
            p = getFreeDataPageId(size);
            if (p == null) {
                throw new IOException("No suitable page found even after adding a new page.");
            }
        }
        return writeRecordToDataPage(record, p);
    }

    public List<Record> GetAllRecords() throws IOException {
        List<Record> all = new ArrayList<>();
        for (PageId pid : getDataPages()) {
            all.addAll(getRecordsInDataPage(pid));
        }
        return all;
    }

    private int getRecordSize(Record record) {
        int size = 0;
        for (int i = 0; i < columnTypes.size(); i++) {
            String type = columnTypes.get(i);
            Object value = record.getValue(i);
            size += getFieldSize(type, value);
        }
        return size;
    }

    private int getFieldSize(String type, Object value) {
        if (type.equals("INT")) {
            return Integer.BYTES;
        } else if (type.equals("REAL")) {
            return Float.BYTES;
        } else if (type.startsWith("CHAR(")) {
            int length = Integer.parseInt(type.substring(5, type.length() - 1));
            return length; 
        } else if (type.startsWith("VARCHAR(")) {
            int maxLen = Integer.parseInt(type.substring(8, type.length() - 1));
            String str = (String) value;
            int actualLen = Math.min(str.length(), maxLen);
            return 4 + actualLen;
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    public int writeRecordToBuffer(Record record, ByteBuffer buffer, int position) {
        buffer.position(position);
        for (int i = 0; i < columnTypes.size(); i++) {
            String type = columnTypes.get(i);
            Object value = record.getValue(i);
            if (type.equals("INT")) {
                buffer.putInt((Integer) value);
            } else if (type.equals("REAL")) {
                buffer.putFloat((Float) value);
            } else if (type.startsWith("CHAR(")) {
                int length = Integer.parseInt(type.substring(5, type.length()-1));
                String s = (String) value;
                if (s.length() > length) s = s.substring(0, length);
                byte[] chars = s.getBytes();
                buffer.put(chars);
                for (int pad = s.length(); pad < length; pad++) {
                    buffer.put((byte)0);
                }
            } else if (type.startsWith("VARCHAR(")) {
                int maxLen = Integer.parseInt(type.substring(8, type.length()-1));
                String s = (String) value;
                int actualLen = Math.min(s.length(), maxLen);
                buffer.putInt(actualLen);
                byte[] chars = s.getBytes();
                buffer.put(chars, 0, actualLen);
            } else {
                throw new IllegalArgumentException("Unknown type: " + type);
            }
        }
        return buffer.position() - position;
    }

    public int readFromBuffer(Record record, ByteBuffer buffer, int position) {
        buffer.position(position);
        record.remove();
        for (String type : columnTypes) {
            if (type.equals("INT")) {
                int v = buffer.getInt();
                record.addValue(v);
            } else if (type.equals("REAL")) {
                float f = buffer.getFloat();
                record.addValue(f);
            } else if (type.startsWith("CHAR(")) {
                int length = Integer.parseInt(type.substring(5, type.length()-1));
                byte[] chars = new byte[length];
                buffer.get(chars);
                String s = new String(chars).replaceAll("\u0000+$", "");
                record.addValue(s);
            } else if (type.startsWith("VARCHAR(")) {
                int len = buffer.getInt();
                byte[] chars = new byte[len];
                buffer.get(chars);
                String s = new String(chars);
                record.addValue(s);
            } else {
                throw new IllegalArgumentException("Unknown type: " + type);
            }
        }
        return buffer.position() - position;
    }

    public String getName() {
        return name;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }

    public void setManagers(DiskManager dm, BufferManager bm, DBConfig conf) {
        this.diskManager = dm;
        this.bufferManager = bm;
        this.config = conf;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public void initHeaderPage() throws IOException {
        ByteBuffer headerBuf = bufferManager.getPage(headerPageId);
        headerBuf.clear();
        headerBuf.putInt(0); // nombre de data pages = 0
        bufferManager.freePage(headerPageId, true);
    }

}
