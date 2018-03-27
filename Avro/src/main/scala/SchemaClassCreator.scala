
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

/**
 * @author sbalai
 * It will be used to create schema classes as per avro.
 * IT will read .avsc file and as per complex hierarchy it will create separate class for each complex type.
 * eg. Say in employee.avsc you have record which has 
 * 1. ID : int
 * 2. Name : String
 * 3. Addresses : Array of Record which has house number,road, pin and all
 * 4. Companies : Array of String
 * 
 * So it will create classes like
 * Employee (ID,name,Address class object,Company object)
 * Address (house number, road, pin)
 * Company (name)
 */
object SchemaClassCreator {

  def main(args: Array[String]): Unit = {
    var schemaLocation = "./first.avsc";
    var destDir = "first";
    if (args.length > 0) {
      schemaLocation = args(0);
      destDir = args(1);
    }
    val schemaFile = new File(schemaLocation);
    val parser = new Schema.Parser();
    val schema = parser.parse(schemaFile);
    val compiler = new SpecificCompiler(schema);
    compiler.compileToDestination(schemaFile, new File(destDir));
    System.out.print("\n avro classes are generated at location '"
      + destDir + "'.");

  }
}
