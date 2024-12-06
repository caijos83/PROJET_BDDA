package miniSGBDR;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Object> values;

    public Record() {
        this.values = new ArrayList<>();
    }

    public Record(List<Object> values) {
        this.values = values;
    }

    public void addValue(Object value) {
        values.add(value);
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public void setValue(int index, Object value) {
        if (index >= 0 && index < values.size()) {
            values.set(index, value);
        } else {
            throw new IndexOutOfBoundsException("Index out of range for record values.");
        }
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = new ArrayList<>(values); // Ensure a new copy is set
    }

    public int size() {
        return values.size();
    }

    public void remove() {
        values.clear();
    }

    public static Record extractRecord(byte[] recordData, List<String> columnTypes) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);
        Record record = new Record();

        for (String columnType : columnTypes) {
            System.out.println("Type attendu : " + columnType);
            System.out.println("Octets restants dans le buffer : " + buffer.remaining());
            if (buffer.remaining() <= 0) {
                throw new IllegalStateException("Pas assez de données dans le buffer pour correspondre aux types attendus !");
            }

            // Extraire la partie principale du type
            String baseType = columnType.split("\\(")[0]; // Ignore les parenthèses

            switch (baseType) {
                case "VARCHAR":
                case "CHAR":
                    if (buffer.remaining() < Integer.BYTES) {
                        throw new IllegalStateException("Pas assez de données pour la longueur de la chaîne !");
                    }
                    int length = buffer.getInt(); // Lire la taille de la chaîne
                    if (buffer.remaining() < length) {
                        throw new IllegalStateException("Pas assez de données pour lire la chaîne !");
                    }
                    byte[] stringBytes = new byte[length];
                    buffer.get(stringBytes);
                    record.addValue(new String(stringBytes));
                    break;

                case "INT":
                    if (buffer.remaining() < Integer.BYTES) {
                        throw new IllegalStateException("Pas assez de données pour un INT !");
                    }
                    record.addValue(buffer.getInt());
                    break;

                case "REAL":
                    if (buffer.remaining() < Float.BYTES) {
                        throw new IllegalStateException("Pas assez de données pour un REAL !");
                    }
                    record.addValue(buffer.getFloat());
                    break;

                case "DOUBLE":
                    if (buffer.remaining() < Double.BYTES) {
                        throw new IllegalStateException("Pas assez de données pour un DOUBLE !");
                    }
                    record.addValue(buffer.getDouble());
                    break;

                default:
                    throw new IllegalArgumentException("Type non supporté : " + columnType);
            }
        }

        return record;
    }






    @Override
    public String toString() {
        return "Record{" + "values=" + values + '}';
    }
}
