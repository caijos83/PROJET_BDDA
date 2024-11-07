//package projet_SGBD;

import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Object> values;

    public Record() {
        this.values = new ArrayList<>();
    }

    public void addValue(Object value) {
        values.add(value);
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public List<Object> getValues() {
        return values;
    }

    public int size() {
        return values.size();
    }
}