package miniSGBDR;

public class Condition {
    private int colLeft;    
    private int colRight;    
    private Object constLeft;
    private Object constRight;
    private Operator op;
    
    
    public Condition(int colIndex, Operator op, Object constant) {
        this.colLeft = colIndex;
        this.colRight = -1;
        this.constLeft = null;
        this.constRight = constant;
        this.op = op;
    }
    
    public Condition(Object constant, Operator op, int colIndex) {
        this.colLeft = -1;
        this.colRight = colIndex;
        this.constLeft = constant;
        this.constRight = null;
        this.op = op;
    }
    
    public Condition(int colIndexLeft, Operator op, int colIndexRight) {
        this.colLeft = colIndexLeft;
        this.colRight = colIndexRight;
        this.constLeft = null;
        this.constRight = null;
        this.op = op;
    }

    private int compareValues(Object v1, Object v2) {
        if (v1 instanceof Integer && v2 instanceof Integer) {
            return ((Integer)v1).compareTo((Integer)v2);
        } else if (v1 instanceof Float && v2 instanceof Float) {
            return ((Float)v1).compareTo((Float)v2);
        } else {
            return v1.toString().compareTo(v2.toString());
        }
    }

    public boolean evaluate(Record r, Relation rel) {
        Object leftVal;
        Object rightVal;

        if (colLeft >= 0) {
            leftVal = r.getValue(colLeft);
        } else {
            leftVal = constLeft;
        }

        if (colRight >= 0) {
            rightVal = r.getValue(colRight);
        } else {
            rightVal = constRight;
        }

        int cmp = compareValues(leftVal, rightVal);
        switch (op) {
            case EQ: return cmp == 0;
            case LT: return cmp < 0;
            case GT: return cmp > 0;
            case LE: return cmp <= 0;
            case GE: return cmp >= 0;
            case NE: return cmp != 0;
        }
        return false;
    }
}
