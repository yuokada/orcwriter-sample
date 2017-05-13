// https://codecheese.wordpress.com/2016/04/20/writing-an-orc-file-using-java/
// https://gist.github.com/omalley
package com.example.orc;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.LongConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

//import org.apache.hadoop.hive.ql.io.orc.OrcFile;

import java.io.IOException;

public class OrcWriter {

  public static void main(String[] args) throws IOException,
                                                InterruptedException {

    String path = "/tmp/file2.orc";
    TypeDescription schema = OrcSchema.newSchema(Paths.get("etc/sample.json"));
    Configuration conf = new Configuration();

    try{
        Writer writer = OrcFile.createWriter(new Path(path),
        OrcFile.writerOptions(conf)
          .setSchema(schema)
          .stripeSize(100000)
          .bufferSize(10000)
          .compress(CompressionKind.ZLIB)
          .version(OrcFile.Version.V_0_12));
      VectorizedRowBatch batch = schema.createRowBatch();
      batch.size = 1;
      ((LongColumnVector) batch.cols[0]).vector[0] = 1;
      ((BytesColumnVector) batch.cols[1]).setVal(0, "hello".getBytes());
      ((BytesColumnVector) batch.cols[2]).setVal(0, "orcFile".getBytes());
      StructColumnVector x = ((StructColumnVector) batch.cols[4]);

      writer.addRowBatch(batch);
      writer.close();

      // cleanup
      Files.delete(Paths.get(path));
    }catch (Exception e){
      e.printStackTrace();
      System.exit(1);
    }
  }
}
