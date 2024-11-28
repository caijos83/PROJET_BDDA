

import java.util.Arrays;
import java.util.List;

public class ConditionTest {
    public static void main(String[] args) {
        // Tests pour les entiers
        System.out.println("=== Tests pour les entiers ===");
        Record recordInt = new Record();
        recordInt.addValue(10);  // Colonne 0
        recordInt.addValue(20);  // Colonne 1
        recordInt.addValue(30);  // Colonne 2

        List<Class<?>> columnTypesInt = Arrays.asList(Integer.class, Integer.class, Integer.class);

        // Test égalité
        Condition conditionIntEq = new Condition(0, "=", 10);
        System.out.println("Test 'égalité' (attendu true): " + conditionIntEq.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        Condition conditionIntEqFalse = new Condition(0, "=", 5);
        System.out.println("Test 'égalité' (attendu false): " + conditionIntEqFalse.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        // Test inférieur à
        Condition conditionIntLt = new Condition(1, "<", 25);
        System.out.println("Test 'inférieur à' (attendu true): " + conditionIntLt.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        Condition conditionIntLtFalse = new Condition(1, "<", 15);
        System.out.println("Test 'inférieur à' (attendu false): " + conditionIntLtFalse.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        // Test supérieur à
        Condition conditionIntGt = new Condition(2, ">", 25);
        System.out.println("Test 'supérieur à' (attendu true): " + conditionIntGt.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        Condition conditionIntGtFalse = new Condition(2, ">", 35);
        System.out.println("Test 'supérieur à' (attendu false): " + conditionIntGtFalse.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        // Test différent de
        Condition conditionIntNe = new Condition(1, "<>", 20);
        System.out.println("Test 'différent de' (attendu false): " + conditionIntNe.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        Condition conditionIntNeFalse = new Condition(1, "<>", 25);
        System.out.println("Test 'différent de' (attendu true): " + conditionIntNeFalse.evaluate(recordInt, Arrays.asList("C1", "C2", "C3"), columnTypesInt));

        // Tests pour les floats
        System.out.println("\n=== Tests pour les floats ===");
        Record recordFloat = new Record();
        recordFloat.addValue(10.5f);  // Colonne 0
        recordFloat.addValue(20.5f);  // Colonne 1
        recordFloat.addValue(30.5f);  // Colonne 2

        List<Class<?>> columnTypesFloat = Arrays.asList(Float.class, Float.class, Float.class);

        // Test égalité
        Condition conditionFloatEq = new Condition(0, "=", 10.5f);
        System.out.println("Test 'égalité' (attendu true): " + conditionFloatEq.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        Condition conditionFloatEqFalse = new Condition(0, "=", 5.5f);
        System.out.println("Test 'égalité' (attendu false): " + conditionFloatEqFalse.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        // Test inférieur à
        Condition conditionFloatLt = new Condition(1, "<", 25.5f);
        System.out.println("Test 'inférieur à' (attendu true): " + conditionFloatLt.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        Condition conditionFloatLtFalse = new Condition(1, "<", 15.5f);
        System.out.println("Test 'inférieur à' (attendu false): " + conditionFloatLtFalse.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        // Test supérieur à
        Condition conditionFloatGt = new Condition(2, ">", 25.5f);
        System.out.println("Test 'supérieur à' (attendu true): " + conditionFloatGt.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        Condition conditionFloatGtFalse = new Condition(2, ">", 35.5f);
        System.out.println("Test 'supérieur à' (attendu false): " + conditionFloatGtFalse.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        // Test différent de
        Condition conditionFloatNe = new Condition(1, "<>", 20.5f);
        System.out.println("Test 'différent de' (attendu false): " + conditionFloatNe.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        Condition conditionFloatNeFalse = new Condition(1, "<>", 25.5f);
        System.out.println("Test 'différent de' (attendu true): " + conditionFloatNeFalse.evaluate(recordFloat, Arrays.asList("C1", "C2", "C3"), columnTypesFloat));

        // Tests pour les chaînes de caractères
        System.out.println("\n=== Tests pour les chaînes de caractères ===");
        Record recordStr = new Record();
        recordStr.addValue("apple");   // Colonne 0
        recordStr.addValue("banana");  // Colonne 1
        recordStr.addValue("cherry");  // Colonne 2

        List<Class<?>> columnTypesStr = Arrays.asList(String.class, String.class, String.class);

        // Test égalité
        Condition conditionStrEq = new Condition(0, "=", "apple");
        System.out.println("Test 'égalité' (attendu true): " + conditionStrEq.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        Condition conditionStrEqFalse = new Condition(0, "=", "grape");
        System.out.println("Test 'égalité' (attendu false): " + conditionStrEqFalse.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        // Test inférieur à
        Condition conditionStrLt = new Condition(1, "<", "cherry");
        System.out.println("Test 'inférieur à' (attendu true): " + conditionStrLt.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        Condition conditionStrLtFalse = new Condition(1, "<", "apple");
        System.out.println("Test 'inférieur à' (attendu false): " + conditionStrLtFalse.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        // Test supérieur à
        Condition conditionStrGt = new Condition(2, ">", "banana");
        System.out.println("Test 'supérieur à' (attendu true): " + conditionStrGt.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        Condition conditionStrGtFalse = new Condition(2, ">", "cherry");
        System.out.println("Test 'supérieur à' (attendu false): " + conditionStrGtFalse.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        // Test différent de
        Condition conditionStrNe = new Condition(0, "<>", "banana");
        System.out.println("Test 'différent de' (attendu true): " + conditionStrNe.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));

        Condition conditionStrNeFalse = new Condition(2, "<>", "cherry");
        System.out.println("Test 'différent de' (attendu false): " + conditionStrNeFalse.evaluate(recordStr, Arrays.asList("C1", "C2", "C3"), columnTypesStr));
    }
}
