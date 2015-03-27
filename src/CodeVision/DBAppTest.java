package CodeVision;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.Iterator;

import libs.BTree;




public class DBAppTest {
	static DBApp testingAll;
	
	public static void main(String[] args) throws DBAppException, IOException, DBEngineException, ClassNotFoundException {
		testingAll = new DBApp();
		
		// createTable Test
		
		testingAll.init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
		htblColNameType.put("ID", "java.lang.Integer");
		htblColNameType.put("major", "java.lang.String");

		Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
		htblColNameRefs.put("ID", "table1.id");

		// ===> execute once and comment createTable and execute multiple
		testingAll.createTable("testCreateTable", htblColNameType, htblColNameRefs,
				"ID");
		testingAll.saveAll();
		
		
		//----------------------------------------------------------------------------------------
		
		// createIndex Test
		/*
		testingAll.init();
		testingAll.createIndex("testCreateTable", "name");
		testingAll.saveAll();
		*/
		
		//----------------------------------------------------------------------------------------
		
		// insertIntoTable Test
		/*
		// Wrong Insertion... Invalid Input Format... NO Effect
		testingAll.init();
		Hashtable<String, String> insertionW1 = new Hashtable<String, String>();
		insertionW1.put("name", "omar");
		insertionW1.put("age", "2");
		//HERE
		insertionW1.put("ID", "ERROR");
		insertionW1.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertionW1);
		testingAll.saveAll();
		
		
		// Wrong Insertion... Column does NOT Exist... NO Effect
		
		testingAll.init();
		Hashtable<String, String> insertionW2 = new Hashtable<String, String>();
		insertionW2.put("name", "omar");
		insertionW2.put("age", "2");
		// HERE
		insertionW2.put("ERROR", "");
		insertionW2.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertionW2);
		testingAll.saveAll();
		
		
		// Right Insertion... Page created... Record Added to page, table, tree
		
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "student");
		insertion.put("age", "20");
		insertion.put("major", "cs");
		insertion.put("ID", "101010");
		testingAll.insertIntoTable("testCreateTable", insertion);
		
		Hashtable<String, String> insertion2 = new Hashtable<String, String>();
		insertion2.put("name", "student2");
		insertion2.put("age", "30");
		insertion2.put("major", "cs");
		insertion2.put("ID", "202020");
		testingAll.insertIntoTable("testCreateTable", insertion2);
		
		Hashtable<String, String> insertion3 = new Hashtable<String, String>();
		insertion3.put("name", "student3");
		insertion3.put("age", "40");
		insertion3.put("major", "DMET");
		insertion3.put("ID", "303030");
		testingAll.insertIntoTable("testCreateTable", insertion3);
		testingAll.saveAll();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());
		
		*/
		//----------------------------------------------------------------------------------------
		
		// deleteFromTable Test
		/*
		testingAll.init();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		BTree x = (BTree) deserialize("data/tables/testCreateTable/BTree/ID.bin");
		x.print();
		
		
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		
		htblColNameValue.put("ID", "202020");
		htblColNameValue.put("name", "student2");
		
		testingAll.deleteFromTable("testCreateTable", htblColNameValue, "AND");
	
	    testingAll.saveAll();
	    
		p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());
		
		x = (BTree) deserialize("data/tables/testCreateTable/BTree/ID.bin");
		x.print();
		*/
		
		
		//----------------------------------------------------------------------------------------
		// selectValueFromTable Test
		
		/*
		testingAll.init();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());
		
		
		// Nothing will be returned... AND
		Hashtable<String, String> select = new Hashtable<String, String>();
		select.put("ID", "101010");
		select.put("name", "student3");
		testingAll.selectValueFromTable("testCreateTable", select, "AND");
		
		
		//AND
		Hashtable<String, String> select2 = new Hashtable<String, String>();
		select2.put("ID", "303030");
		select2.put("name", "student3");
		testingAll.selectValueFromTable("testCreateTable", select2, "AND");
		
		
		//OR
		Hashtable<String, String> select3 = new Hashtable<String, String>();
		select3.put("ID", "101010");
		select3.put("name", "student3");
		testingAll.selectValueFromTable("testCreateTable", select3, "OR");
		
		testingAll.saveAll();
		*/
		
		
		//----------------------------------------------------------------------------------------
		
		//selectRangeFromTable Test
		/*
		testingAll.init();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		Hashtable<String, String> select = new Hashtable<String, String>();
		select.put("age", ">=30");
		Iterator I = testingAll.selectRangeFromTable("testCreateTable", select,
				"AND");

		while (I.hasNext()) {
			System.out.println("done " + I.next().toString());
		}
		*/
		
		//----------------------------------------------------------------------------------------
		// saveAll Test
		/*
		testingAll.init();
		Hashtable<String, String> insertionWithSave = new Hashtable<String, String>();
		insertionWithSave.put("name", "student with save");
		insertionWithSave.put("age", "40");
		insertionWithSave.put("major", "save test");
		insertionWithSave.put("ID", "454");
		testingAll.insertIntoTable("testCreateTable", insertionWithSave);
		testingAll.saveAll();
		
		Hashtable<String, String> insertionWithoutSave = new Hashtable<String, String>();
		insertionWithoutSave.put("name", "student without save");
		insertionWithoutSave.put("age", "40");
		insertionWithoutSave.put("major", "save test NOT");
		insertionWithoutSave.put("ID", "2956");
		testingAll.insertIntoTable("testCreateTable", insertionWithoutSave);
		
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());
		*/
		
		
	}	
	
	
	public static Object deserialize(String path) throws IOException,
			ClassNotFoundException {
		FileInputStream fi = new FileInputStream(path);
		ObjectInputStream os = new ObjectInputStream(fi);
		Object x = os.readObject();
		// System.out.println(((Page)x).getRecords().get(0));
		os.close();
		fi.close();
		return x;
	}
}
