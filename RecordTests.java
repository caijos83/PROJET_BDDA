package miniSGBDR;

import java.util.Arrays;

public class RecordTests {
    public static void main(String[] args) {
        testAddValue();
        testGetValue();
        testSetValues();
        testClearValues();
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
}
