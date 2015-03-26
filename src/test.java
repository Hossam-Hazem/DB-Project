import java.io.File;
import java.lang.reflect.Constructor;


public class test {
	public static void main(String[] args) {
		String strColType = "java.lang.Integer";
		String strColValue = "100";
		Class c = Class.forName( strColType );
		Constructor constructor = c.getConstructor(strColValue);
		strColValue = constructor.newInstance( );
	}
}
