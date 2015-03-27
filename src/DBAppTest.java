import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import BPTree.BTree;


public class DBAppTest {
	static DBApp testingAll;
	
	public static void main(String[] args) throws DBAppException, IOException, DBEngineException, ClassNotFoundException {
		testingAll = new DBApp();
		
		// createTable Test
		/*
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
		*/
		
		//----------------------------------------------------------------------------------------
		
		// createIndex Test
		/*
		testingAll.init();
		testingAll.createIndex("testCreateTable", "name");
		testingAll.saveAll();
		*/
		
		//----------------------------------------------------------------------------------------
		
		// insertIntoTable Test
		
		// Wrong Insertion... Invalid Input Format... NO Effect
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "omar");
		insertion.put("age", "2");
		//HERE
		insertion.put("ID", "ERROR");
		insertion.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertion);
		testingAll.saveAll();
		*/
		
		// Wrong Insertion... Column does NOT Exist... NO Effect
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "omar");
		insertion.put("age", "2");
		// HERE
		insertion.put("ERROR", "");
		insertion.put("major", "cs");
		testingAll.insertIntoTable("testCreateTable", insertion);
		testingAll.saveAll();
		*/
		
		// Right Insertion... Page created... Record Added to page, table, tree
		/*
		testingAll.init();
		Hashtable<String, String> insertion = new Hashtable<String, String>();
		insertion.put("name", "student");
		insertion.put("age", "20");
		insertion.put("major", "cs");
		insertion.put("ID", "10101010");
		testingAll.insertIntoTable("testCreateTable", insertion);
		
		Hashtable<String, String> insertion2 = new Hashtable<String, String>();
		insertion2.put("name", "student2");
		insertion2.put("age", "30");
		insertion2.put("major", "cs");
		insertion2.put("ID", "20202020");
		testingAll.insertIntoTable("testCreateTable", insertion2);
		
		Hashtable<String, String> insertion3 = new Hashtable<String, String>();
		insertion3.put("name", "student3");
		insertion3.put("age", "40");
		insertion3.put("major", "DMET");
		insertion3.put("ID", "30303030");
		testingAll.insertIntoTable("testCreateTable", insertion3);
		testingAll.saveAll();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());
		*/
		
		//----------------------------------------------------------------------------------------
		
		// deleteFromTable Test
		
		DBApp.init();
		Page p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		BTree x = (BTree) deserialize("data/tables/testCreateTable/BTree/ID.bin");
		x.print();
		
		// NO Effect
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("ID", "10101010");
		htblColNameValue.put("name", "student2");
		testingAll.deleteFromTable("testCreateTable", htblColNameValue, "OR");

		p = (Page) deserialize("data/tables/testCreateTable/pages/0.class");
		System.out.println("All Records: " + p.getRecords());

		x = (BTree) deserialize("data/tables/testCreateTable/BTree/ID.bin");
		x.print();
		testingAll.saveAll();

		
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
