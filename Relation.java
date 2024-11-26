import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation implements Serializable {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"
    private List<Record> records;
    private transient PageId headerPageId; // Identifiant de la page d'en-tête (transient pour sérialisation)

    public Relation(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Relation name cannot be null or empty.");
        }
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.records = new ArrayList<>();
    }

    public void addColumn(String name, String type) {
        // // Vérifie si le nom de colonne est valide
        // if (name == null || name.trim().isEmpty()) {
        // 	throw new IllegalArgumentException("Column name cannot be null or empty.");
        // }
        // if (!name.matches("[A-Za-z][A-Za-z0-9_]*")) {
        // 	throw new IllegalArgumentException("Invalid column name: " + name);
        // }

        // // Vérifie si le type est valide
        // if (type == null || type.trim().isEmpty()) {
        // 	throw new IllegalArgumentException("Column type cannot be null or empty.");
        // }

        // // Regex pour INT, REAL, CHAR(n), et VARCHAR(n)
        // if (!type.matches("INT|REAL|CHAR\\(\\d+\\)|VARCHAR\\(\\d+\\)")) {
        // 	throw new IllegalArgumentException("Invalid column type: " + type);
        // }

        columnNames.add(name);
        columnTypes.add(type);
    }

    public int writeRecordToBuffer(Record record, ByteBuffer buffer, int position) {
        if (record.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Record size does not match the number of columns.");
        }
        if (position < 0 || position >= buffer.capacity()) {
            throw new IllegalArgumentException("Invalid buffer position: " + position);
        }

        buffer.position(position);
        int totalSize = 0;

        try {
            for (int i = 0; i < record.size(); i++) {
                Object value = record.getValue(i);
                String type = columnTypes.get(i);

                if (type.equals("INT")) {
                    if (!(value instanceof Integer)) {
                        throw new IllegalArgumentException("Expected INT but found " + value.getClass().getSimpleName());
                    }
                    buffer.putInt((Integer) value);
                    totalSize += Integer.BYTES;

                } else if (type.equals("REAL")) {
                    if (!(value instanceof Float)) {
                        throw new IllegalArgumentException("Expected REAL but found " + value.getClass().getSimpleName());
                    }
                    buffer.putFloat((Float) value);
                    totalSize += Float.BYTES;

                } else if (type.startsWith("CHAR")) {
                    int length = Integer.parseInt(type.substring(5, type.length() - 1));
                    if (!(value instanceof String)) {
                        throw new IllegalArgumentException("Expected CHAR but found " + value.getClass().getSimpleName());
                    }
                    String strValue = (String) value;
                    if (strValue.length() > length) {
                        throw new IllegalArgumentException("CHAR value exceeds specified length: " + length);
                    }
                    for (char c : strValue.toCharArray()) {
                        buffer.put((byte) c);
                    }
                    for (int j = strValue.length(); j < length; j++) {
                        buffer.put((byte) 0); // Null character padding
                    }
                    totalSize += length;

                } else if (type.startsWith("VARCHAR")) {
                    if (!(value instanceof String)) {
                        throw new IllegalArgumentException("Expected VARCHAR but found " + value.getClass().getSimpleName());
                    }
                    String strValue = (String) value;
                    buffer.putInt(strValue.length()); // Length of the string
                    for (char c : strValue.toCharArray()) {
                        buffer.put((byte) c);
                    }
                    totalSize += 4 + strValue.length();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error writing record to buffer: " + e.getMessage(), e);
        }

        return totalSize;
    }

    public int readFromBuffer(Record record, ByteBuffer buffer, int position) {
        if (record == null) {
            throw new IllegalArgumentException("Record cannot be null.");
        }
        if (position < 0 || position >= buffer.capacity()) {
            throw new IllegalArgumentException("Invalid buffer position: " + position);
        }

        buffer.position(position);
        int totalSize = 0;

        try {
            for (String type : columnTypes) {
                if (type.equals("INT")) {
                    record.addValue(buffer.getInt());
                    totalSize += Integer.BYTES;

                } else if (type.equals("REAL")) {
                    record.addValue(buffer.getFloat());
                    totalSize += Float.BYTES;

                } else if (type.startsWith("CHAR")) {
                    int length = Integer.parseInt(type.substring(5, type.length() - 1));
                    byte[] charBytes = new byte[length];
                    buffer.get(charBytes);
                    String strValue = new String(charBytes).replaceAll("\u0000+$", "");
                    record.addValue(strValue);
                    totalSize += length;

                } else if (type.startsWith("VARCHAR")) {
                    int strLength = buffer.getInt();
                    byte[] strBytes = new byte[strLength];
                    buffer.get(strBytes);
                    String strValue = new String(strBytes);
                    record.addValue(strValue);
                    totalSize += 4 + strLength;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error reading record from buffer: " + e.getMessage(), e);
        }

        return totalSize;
    }

    public void saveHeaderPageId(ObjectOutputStream oos) throws IOException {
        if (headerPageId != null) {
            oos.writeInt(headerPageId.getFileIdx());
            oos.writeInt(headerPageId.getPageIdx());
        } else {
            oos.writeInt(-1); // Special marker indicating no header page
        }
    }

    public void loadHeaderPageId(ObjectInputStream ois) throws IOException {
        int fileIdx = ois.readInt();
        if (fileIdx != -1) {
            int pageIdx = ois.readInt();
            headerPageId = new PageId(fileIdx, pageIdx);
        }
    }

    public int getColumnNumber() {
        return columnNames.size();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void addRecord(Record record) {
        records.add(record);
    }
}
