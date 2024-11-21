import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;


public class Relation implements Serializable {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"
    private List<Record> records;
    private transient PageId headerPageId; // Identifiant de la page d'en-tête (transient pour sérialisation)



    public Relation(String name) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
    }

    public void addColumn(String name, String type) {
        columnNames.add(name);
        columnTypes.add(type);
    }

    public int writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);
        int totalSize = 0;
        for (int i = 0; i < record.size(); i++) {
            Object value = record.getValue(i);
            String type = columnTypes.get(i);
            if (type.equals("INT")) {
                buff.putInt((Integer) value);
                totalSize += Integer.BYTES;
            } else if (type.equals("REAL")) {
                buff.putFloat((Float) value);
                totalSize += Float.BYTES;
            } else if (type.startsWith("CHAR")) {
                int length = Integer.parseInt(type.substring(5, type.length() - 1)); // Extrait T
                String strValue = (String) value;
                for (char c : strValue.toCharArray()) {
                    buff.put((byte) c);
                }
                for (int j = strValue.length(); j < length; j++) {
                    buff.put((byte) 0); // Caractère nul
                }
                totalSize += length;
            } else if (type.startsWith("VARCHAR")) {
                String strValue = (String) value;
                buff.putInt(strValue.length()); // Longueur de la chaîne
                for (char c : strValue.toCharArray()) {
                    buff.put((byte) c);
                }
                totalSize += 4 + strValue.length(); // 4 octets pour la longueur + longueur de la chaîne
            }
        }

        return totalSize;
    }



    public int readFromBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);
        int totalSize = 0;

        for (String type : columnTypes) {
            if (type.equals("INT")) {
                record.addValue(buff.getInt());
                totalSize += Integer.BYTES;
            } else if (type.equals("REAL")) {
                record.addValue(buff.getFloat());
                totalSize += Float.BYTES;
            } else if (type.startsWith("CHAR")) {
                int length = Integer.parseInt(type.substring(5, type.length() - 1));
                byte[] charBytes = new byte[length];
                buff.get(charBytes);
                String strValue = new String(charBytes).replaceAll("\u0000$", ""); // Supprime les caractères nuls
                record.addValue(strValue);
                totalSize += length;
            } else if (type.startsWith("VARCHAR")) {
                int strLength = buff.getInt();
                byte[] strBytes = new byte[strLength];
                buff.get(strBytes);
                String strValue = new String(strBytes);
                record.addValue(strValue);
                totalSize += 4 + strLength; // 4 pour la longueur + longueur de la chaîne
            }
        }

        return totalSize;
    }

    public void saveHeaderPageId(ObjectOutputStream oos) throws IOException {
        if (headerPageId != null) {
            oos.writeInt(headerPageId.getFileIdx());
            oos.writeInt(headerPageId.getPageIdx());
        } else {
            oos.writeInt(-1); // Marqueur spécial indiquant qu'il n'y a pas de page d'en-tête
        }
    }

    // Méthode pour charger headerPageId
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
        // Cette méthode devrait retourner tous les enregistrements associés à cette relation
        return records; // Exemple si `records` est une variable membre
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void addRecord(Record record) {
    }
}
