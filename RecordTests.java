package miniSGBDR;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RecordTests {
    public static void main(String[] args) {
        testAddValue();
        testGetValue();
        testSetValues();
        testClearValues();
        testExtractRecord();
    }

    public static void testAddValue() {
        Record record = new Record();
        record.addValue(1);
        record.addValue("Alice");
        System.out.println("testAddValue: " + record.getValues());
        assert record.getValues().equals(Arrays.asList(1, "Alice")) : "Failed to add values!";
    }

    public static void testGetValue() {
        Record record = new Record();
        record.addValue(1);
        record.addValue("Alice");
        Object value = record.getValue(1);
        System.out.println("testGetValue: " + value);
        assert value.equals("Alice") : "Failed to get value at index!";
    }

    public static void testSetValues() {
        Record record = new Record();
        record.setValues(Arrays.asList(2, "Bob", 25));
        System.out.println("testSetValues: " + record.getValues());
        assert record.getValues().equals(Arrays.asList(2, "Bob", 25)) : "Failed to set values!";
    }

    public static void testClearValues() {
        Record record = new Record();
        record.addValue(1);
        record.addValue("Alice");
        record.remove();
        System.out.println("testClearValues: " + record.getValues());
        assert record.getValues().isEmpty() : "Failed to clear values!";
    }

    public static void testExtractRecord() {
        // Étape 1 : Créer une liste de valeurs pour un record
        List<Object> values = Arrays.asList(1, "Alice", 30.5f);

        // Étape 2 : Définir les types des colonnes
        List<String> columnTypes = Arrays.asList("INT", "VARCHAR", "REAL");

        // Étape 3 : Créer un record original
        Record originalRecord = new Record(values);

        // Étape 4 : Sérialiser le record en bytes
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            // Simulation de l'écriture du record dans le buffer
            buffer.putInt(1); // INT
            String name = "Alice";
            buffer.putInt(name.length()); // Taille de la chaîne
            buffer.put(name.getBytes()); // Bytes de la chaîne
            buffer.putFloat(30.5f); // REAL
        } catch (Exception e) {
            System.err.println("Erreur lors de la sérialisation : " + e.getMessage());
            return;
        }

        // Récupérer les bytes sérialisés
        byte[] serializedRecord = new byte[buffer.position()];
        buffer.flip();
        buffer.get(serializedRecord);

        // Étape 5 : Désérialiser le record à partir des bytes
        Record deserializedRecord = null;
        try {
            deserializedRecord = Record.extractRecord(serializedRecord, columnTypes);
        } catch (Exception e) {
            System.err.println("Erreur lors de la désérialisation : " + e.getMessage());
            return;
        }

        // Étape 6 : Vérifier que les valeurs du record désérialisé sont identiques à l'original
        System.out.println("Original Record: " + originalRecord.getValues());
        System.out.println("Deserialized Record: " + deserializedRecord.getValues());
        assert originalRecord.getValues().equals(deserializedRecord.getValues()) : "Les valeurs du record désérialisé ne correspondent pas !";

        System.out.println("testExtractRecord passé !");
    }

}
