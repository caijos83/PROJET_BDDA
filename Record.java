package miniSGBDR;

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
        this.values = new ArrayList<>(values);
    }

    public int size() {
        return values.size();
    }

    public void remove() {
        values.clear();
    }

    @Override
    public String toString() {
        return "Record{" + "values=" + values + '}';
    }
}
