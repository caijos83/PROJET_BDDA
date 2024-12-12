package miniSGBDR;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.io.IOException;

public class SGBD {
    private DBManager dbManager;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBConfig config;

    public SGBD(DBConfig config) throws IOException, ClassNotFoundException {
        this.config = config;
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
        this.dbManager = new DBManager(config, diskManager, bufferManager);
        
        // Charger l'état
        diskManager.LoadState();
        dbManager.LoadState();
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("? ");
            String command = scanner.nextLine().trim();
            if (command.equalsIgnoreCase("QUIT")) {
                processQuitCommand();
                break;
            }
            processCommand(command);
        }
        scanner.close();
    }

    private void processCommand(String command) {
        try {
            if (command.startsWith("CREATE DATABASE ")) {
                String dbName = command.substring("CREATE DATABASE ".length()).trim();
                dbManager.CreateDatabase(dbName);
            } else if (command.startsWith("SET DATABASE ")) {
                String dbName = command.substring("SET DATABASE ".length()).trim();
                dbManager.SetCurrentDatabase(dbName);
            } else if (command.startsWith("CREATE TABLE ")) {
                processCreateTableCommand(command);
            } else if (command.startsWith("DROP TABLE ")) {
                String tableName = command.substring("DROP TABLE ".length()).trim();
                dbManager.DropTable(tableName);
            } else if (command.equals("LIST TABLES")) {
                dbManager.ListTablesInCurrentDatabase();
            } else if (command.equals("DROP TABLES")) {
                dbManager.RemoveTablesFromCurrentDatabase();
            } else if (command.equals("DROP DATABASES")) {
                dbManager.RemoveDatabases();
            } else if (command.equals("LIST DATABASES")) {
                dbManager.ListDatabases();
            } else if (command.startsWith("INSERT INTO ")) {
                processInsertCommand(command);
            } else if (command.startsWith("BULKINSERT INTO ")) {
                processBulkInsertCommand(command);
            } else if (command.startsWith("SELECT ")) {
                processSelectCommand(command);
            } else {
                System.out.println("Unknown command: " + command);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private void processQuitCommand() {
        try {
            dbManager.SaveState();
            diskManager.SaveState();
            bufferManager.flushBuffers();
            System.out.println("All state saved. Bye.");
        } catch (IOException e) {
            System.out.println("Error during quit: " + e.getMessage());
        }
    }

    private void processCreateTableCommand(String command) throws Exception {
        int startParen = command.indexOf('(');
        int endParen = command.lastIndexOf(')');
        if (startParen == -1 || endParen == -1 || endParen < startParen) {
            throw new Exception("Invalid CREATE TABLE command format.");
        }

        String prefix = command.substring(0, startParen).trim();
        String[] parts = prefix.split(" ");
        if (parts.length < 3) {
            throw new Exception("Invalid CREATE TABLE syntax.");
        }
        String tableName = parts[2].trim();

        String cols = command.substring(startParen + 1, endParen).trim();
        String[] colDefs = cols.split(",");
        List<String> colNames = new ArrayList<>();
        List<String> colTypes = new ArrayList<>();

        for (String cdef : colDefs) {
            String[] cparts = cdef.split(":");
            if (cparts.length != 2) {
                throw new Exception("Invalid column definition: " + cdef);
            }
            colNames.add(cparts[0].trim());
            colTypes.add(cparts[1].trim());
        }

        dbManager.CreateTable(tableName, colNames, colTypes);
        System.out.println("Table " + tableName + " created.");
    }

    private void processInsertCommand(String command) throws Exception {
        int intoIndex = command.indexOf("INTO");
        int valuesIndex = command.indexOf("VALUES");
        if (intoIndex < 0 || valuesIndex < 0) {
            throw new Exception("Invalid INSERT command");
        }

        String partRelation = command.substring(intoIndex + 4, valuesIndex).trim(); 
        String nomRelation = partRelation;

        String partValues = command.substring(valuesIndex + 6).trim();
        if (!partValues.startsWith("(") || !partValues.endsWith(")")) {
            throw new Exception("Invalid VALUES format");
        }

        String inner = partValues.substring(1, partValues.length()-1).trim();
        String[] vals = inner.split(",");

        Relation rel = dbManager.GetTableFromCurrentDatabase(nomRelation);
        if (rel == null) {
            throw new Exception("Table not found: " + nomRelation);
        }

        if (vals.length != rel.getColumnCount()) {
            throw new Exception("Number of values does not match number of columns.");
        }

        Record record = new Record();
        for (int i = 0; i < vals.length; i++) {
            String colType = rel.getColumnTypes().get(i);
            Object value = parseValue(vals[i].trim(), colType);
            record.addValue(value);
        }

        rel.InsertRecord(record);
        System.out.println("1 record inserted into " + nomRelation);
    }

    private void processBulkInsertCommand(String command) throws Exception {
        int intoIdx = command.indexOf("INTO");
        if (intoIdx < 0) throw new Exception("Invalid BULKINSERT command");

        String afterInto = command.substring(intoIdx + 4).trim();
        String[] parts = afterInto.split(" ");
        if (parts.length != 2) throw new Exception("Invalid BULKINSERT format");
        String nomRelation = parts[0].trim();
        String csvFile = parts[1].trim();

        Relation rel = dbManager.GetTableFromCurrentDatabase(nomRelation);
        if (rel == null) {
            throw new Exception("Table not found: " + nomRelation);
        }

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int count = 0;
        while ((line = br.readLine()) != null) {
            String[] vals = line.split(";");
            if (vals.length != rel.getColumnCount()) {
                throw new Exception("CSV line does not match column count.");
            }
            Record record = new Record();
            for (int i = 0; i < vals.length; i++) {
                String colType = rel.getColumnTypes().get(i);
                Object value = parseValue(vals[i].trim(), colType);
                record.addValue(value);
            }
            rel.InsertRecord(record);
            count++;
        }
        br.close();
        System.out.println(count + " records inserted into " + nomRelation + " from " + csvFile);
    }

    private void processSelectCommand(String command) throws Exception {
        String upper = command.toUpperCase();
        int fromIdx = upper.indexOf("FROM");
        if (fromIdx < 0) throw new Exception("Missing FROM");

        String selectPart = command.substring("SELECT".length(), fromIdx).trim();
        String afterFrom = command.substring(fromIdx + 4).trim();

        String[] fromParts = afterFrom.split(" ");
        if (fromParts.length < 2) throw new Exception("FROM syntax error");
        String nomRelation = fromParts[0];
        String alias = fromParts[1];

        Relation rel = dbManager.GetTableFromCurrentDatabase(nomRelation);
        if (rel == null) throw new Exception("Table not found: " + nomRelation);

        List<Integer> projCols = new ArrayList<>();
        boolean selectAll = false;
        if (selectPart.equals("*")) {
            selectAll = true;
            for (int i = 0; i < rel.getColumnCount(); i++) {
                projCols.add(i);
            }
        } else {
            String[] colNames = selectPart.split(",");
            for (String c : colNames) {
                c = c.trim();
                String colName = c.substring(c.indexOf('.')+1);
                int colIdx = rel.getColumnNames().indexOf(colName);
                if (colIdx < 0) throw new Exception("Unknown column: " + colName);
                projCols.add(colIdx);
            }
        }

        List<Condition> conditions = new ArrayList<>();
        int whereIdx = upper.indexOf("WHERE");
        if (whereIdx >= 0) {
            String wherePart = command.substring(whereIdx + 5).trim();
            String[] condParts = wherePart.split(" AND ");
            for (String cp : condParts) {
                conditions.add(parseCondition(cp, rel, alias));
            }
        }

        IRecordIterator scan = new RelationScanner(rel);
        if (!conditions.isEmpty()) {
            scan = new SelectOperator(rel, scan, conditions);
        }

        if (!selectAll) {
            scan = new ProjectOperator(scan, projCols);
        }

        RecordPrinter printer = new RecordPrinter(scan);
        printer.printAll();
    }

    private Condition parseCondition(String c, Relation rel, String alias) throws Exception {
        String[] ops = {"<=", ">=", "<>", "=", "<", ">"};
        String chosenOp = null;
        for (String o : ops) {
            int idx = c.indexOf(o);
            if (idx >= 0) {
                chosenOp = o;
                break;
            }
        }
        if (chosenOp == null) throw new Exception("No operator found in condition: " + c);
        Operator op = parseOperator(chosenOp);
        int opPos = c.indexOf(chosenOp);
        String leftTerm = c.substring(0, opPos).trim();
        String rightTerm = c.substring(opPos + chosenOp.length()).trim();

        int leftCol = parseTerm(leftTerm, rel, alias);
        int rightCol = parseTerm(rightTerm, rel, alias);

        Object leftConst = null;
        Object rightConst = null;
        if (leftCol < 0) leftConst = parseConstant(leftTerm, rel);
        if (rightCol < 0) rightConst = parseConstant(rightTerm, rel);

        if (leftCol >= 0 && rightCol >= 0) {
            return new Condition(leftCol, op, rightCol);
        } else if (leftCol >= 0 && rightConst != null) {
            return new Condition(leftCol, op, rightConst);
        } else if (leftConst != null && rightCol >= 0) {
            return new Condition(leftConst, op, rightCol);
        } else {
            throw new Exception("Invalid condition: both terms are constants?");
        }
    }

    private Operator parseOperator(String op) {
        switch (op) {
            case "=": return Operator.EQ;
            case "<": return Operator.LT;
            case ">": return Operator.GT;
            case "<=": return Operator.LE;
            case ">=": return Operator.GE;
            case "<>": return Operator.NE;
            default: return null;
        }
    }

    private int parseTerm(String t, Relation rel, String alias) {
        if (t.startsWith(alias + ".")) {
            String colName = t.substring(alias.length()+1);
            return rel.getColumnNames().indexOf(colName);
        }
        return -1;
    }

    private Object parseConstant(String val, Relation rel) {
        try {
            return Integer.valueOf(val);
        } catch (NumberFormatException e1) {
            try {
                return Float.valueOf(val);
            } catch (NumberFormatException e2) {
                return val; // string
            }
        }
    }

    private Object parseValue(String val, String colType) {
        if (colType.equals("INT")) {
            return Integer.parseInt(val);
        } else if (colType.equals("REAL")) {
            return Float.parseFloat(val);
        } else {
            return val;
        }
    }

    public static void main(String[] args) {
        try {
            DBConfig config;
            if (args.length > 0) {
                config = DBConfig.loadDBConfig(args[0]);
            } else {
                // Config par défaut
                config = new DBConfig("./DB", 4096, 20480, DBConfig.BMpolicy.LRU, 10);
            }
            SGBD sgbd = new SGBD(config);
            sgbd.run();
        } catch (Exception e) {
            System.err.println("Error starting SGBD: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
