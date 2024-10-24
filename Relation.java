//package projet_SGBD;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes; // "INT", "REAL", "CHAR(T)", "VARCHAR(T)"

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
    
    public int getColumnNumber() {
        return columnNames.size();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }
}
    
