import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
public class DeIdentifyData {
    static Logger logger = Logger.getLogger(DeIdentifyData.class.getName());

    public static Integer[] Columns = {2,3,4,5,6,8}; // Columns will be encryption
    private static byte[] encryptionKey = new String("teamBaDao").getBytes(); // Encryption key
    public static class Map extends Mapper<Object, Text, NullWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			List<Integer> list = new ArrayList<Integer>();
			Collections.addAll(list, Columns);
			String newString = "";
			int counter = 1;
			while (itr.hasMoreTokens()) { // Checks whether the current column being processed is one of the columns to be encrypted, as specified in the Columns array.
				String token = itr.nextToken();
				if(list.contains(counter)) {
					if(newString.length() > 0)
						newString += ",";
					newString += encrypt(token, encryptionKey); // Encrypt token and append to the  "newString"
				} else {
					if(newString.length() > 0)
						newString += ",";
					newString += token; // The original token value is appened to the "newString"
				}
				counter = counter + 1;
			}
			context.write(NullWritable.get(), new Text(newString.toString())); 
		}
	}

    public static String encrypt(String encryptString, byte[] key) { // Ecryption
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            String encryptedString = Base64.encodeBase64String(cipher.doFinal(encryptString.getBytes()));
            return encryptedString.trim();
        } catch (Exception e) {
            logger.error("Error encrypting", e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.exit(-1);
        }
		Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		// if the output path already exists, delete it
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setJarByClass(DeIdentifyData.class);
        job.waitForCompletion(true);
    }
}
