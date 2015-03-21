import java.io.File;
import java.util.ArrayList;


public class Table {
	ArrayList<Page> Pages;
	String name;
	int InitIndex ;
	public Table(String n){
		name = n;
		File x = new File("Data\\"+name);
		x.mkdir();
		Pages.add(new Page(0));
		InitIndex=0;
	}
	
}
