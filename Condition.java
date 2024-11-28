import java.util.List;

public class Condition {
    private int columnIndex;   // Index de la colonne dans le tuple
    private String operator;   // L'opérateur de comparaison (=, <, >, etc.)
    private Object value;      // Valeur de la condition (peut être une constante de type Object)

    // Constructeur
    public Condition(int columnIndex, String operator, Object value) {
        this.columnIndex = columnIndex;
        this.operator = operator;
        this.value = value;
    }

    // Méthode d'évaluation de la condition sur un record
    public boolean evaluate(Record record, List<String> columnNames, List<Class<?>> columnTypes) {
        Object recordValue = record.getValue(columnIndex); // Valeur de la colonne à vérifier
        Class<?> columnType = columnTypes.get(columnIndex); // Type de la colonne

        // Comparaison avec la valeur, en tenant compte du type de la colonne
        switch (operator) {
            case "=":
                return compareEquals(recordValue, columnType);
            case "<":
                return compareLessThan(recordValue, columnType);
            case ">":
                return compareGreaterThan(recordValue, columnType);
            case "<=":
                return compareLessThanOrEqual(recordValue, columnType);
            case ">=":
                return compareGreaterThanOrEqual(recordValue, columnType);
            case "<>":
                return compareNotEqual(recordValue, columnType);
            default:
                throw new IllegalArgumentException("Opérateur non pris en charge: " + operator);
        }
    }

    // Compare égalité en fonction du type de la colonne
    private boolean compareEquals(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return ((Integer) recordValue).equals(value);
        } else if (columnType == Float.class) {
        	return ((Float) recordValue).equals(value);
        }else if (columnType == String.class) {
            return ((String) recordValue).equals(value);
        }
        // Ajouter d'autres types si nécessaire (Float, Date, etc.)
        throw new IllegalArgumentException("Type non supporté pour '=': " + columnType.getName());
    }

    // Compare "inférieur à" en fonction du type de la colonne
    private boolean compareLessThan(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return ((Integer) recordValue) < (Integer) value;
        }else if (columnType == Float.class) {
        	return ((Float) recordValue) < (Float) value;
        } else if (columnType == String.class) {
            return ((String) recordValue).compareTo((String) value) < 0;
        }
        // Ajouter d'autres types si nécessaire
        throw new IllegalArgumentException("Type non supporté pour '<': " + columnType.getName());
    }

    // Compare "supérieur à" en fonction du type de la colonne
    private boolean compareGreaterThan(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return ((Integer) recordValue) > (Integer) value;
        } else if (columnType == Float.class) {
        	return ((Float) recordValue) > (Float) value;
        }else if (columnType == String.class) {
            return ((String) recordValue).compareTo((String) value) > 0;
        }
        // Ajouter d'autres types si nécessaire
        throw new IllegalArgumentException("Type non supporté pour '>': " + columnType.getName());
    }

    // Compare "inférieur ou égal à" en fonction du type de la colonne
    private boolean compareLessThanOrEqual(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return ((Integer) recordValue) <= (Integer) value;
        } else if (columnType == Float.class) {
        	return ((Float) recordValue) <= (Float) value;
        }else if (columnType == String.class) {
            return ((String) recordValue).compareTo((String) value) <= 0;
        }
        // Ajouter d'autres types si nécessaire
        throw new IllegalArgumentException("Type non supporté pour '<=': " + columnType.getName());
    }

    // Compare "supérieur ou égal à" en fonction du type de la colonne
    private boolean compareGreaterThanOrEqual(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return ((Integer) recordValue) >= (Integer) value;
        } else if (columnType == Float.class) {
        	return ((Float) recordValue) >= (Float) value;
        }else if (columnType == String.class) {
            return ((String) recordValue).compareTo((String) value) >= 0;
        }
        // Ajouter d'autres types si nécessaire
        throw new IllegalArgumentException("Type non supporté pour '>=': " + columnType.getName());
    }

    // Compare "différent de" en fonction du type de la colonne
    private boolean compareNotEqual(Object recordValue, Class<?> columnType) {
        if (columnType == Integer.class) {
            return !((Integer) recordValue).equals(value);
        } else if (columnType == Float.class) {
        	return !((Float) recordValue).equals(value);
        }else if (columnType == String.class) {
            return !((String) recordValue).equals(value);
        }
        // Ajouter d'autres types si nécessaire
        throw new IllegalArgumentException("Type non supporté pour '<>': " + columnType.getName());
    }
}



