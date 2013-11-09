
import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;
import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import javax.xml.stream.*;

public class XMLParser
{

        public static class XmlInputFormat1 extends TextInputFormat {

        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";


        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new XmlRecordReader();
        }

        /**
         * XMLRecordReader class to read through a given xml document to output
         * xml blocks as records as specified by the start tag and end tag
         *
         */
       
        public static class XmlRecordReader extends
                RecordReader<LongWritable, Text> {
            private byte[] startTag;
            private byte[] endTag;
            private long start;
            private long end;
            private FSDataInputStream fsin;
            private DataOutputBuffer buffer = new DataOutputBuffer();

            private LongWritable key = new LongWritable();
            private Text value = new Text();
                @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
                endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
                FileSplit fileSplit = (FileSplit) split;

                // open the file and seek to the start of the split
                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                fsin = fs.open(fileSplit.getPath());
                fsin.seek(start);

            }
        @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                if (fsin.getPos() < end) {
                    if (readUntilMatch(startTag, false)) {
                        try {
                            buffer.write(startTag);
                            if (readUntilMatch(endTag, true)) {
                                key.set(fsin.getPos());
                                value.set(buffer.getData(), 0,
                                        buffer.getLength());
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    }
                }
                return false;
            }
        @Override
           public LongWritable getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

        @Override
            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }
        @Override
            public void close() throws IOException {
                fsin.close();
            }
        @Override
            public float getProgress() throws IOException {
                return (fsin.getPos() - start) / (float) (end - start);
            }

            private boolean readUntilMatch(byte[] match, boolean withinBlock)
                    throws IOException {
                int i = 0;
                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1)
                        return false;
                    // save to buffer:
                    if (withinBlock)
                        buffer.write(b);
                    // check if we're matching:
                    if (b == match[i]) {
                        i++;
                        if (i >= match.length)
                            return true;
                    } else
                        i = 0;
                    // see if we've passed the stop point:
                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
                        return false;
                }
            }
        }
    }


        public static class Map extends Mapper<LongWritable, Text,
    Text, Text> {
  @Override
  protected void map(LongWritable key, Text value,
                     Mapper.Context context)
      throws
      IOException, InterruptedException {
    String document = value.toString();
    System.out.println("‘" + document + "‘");
        try {
      XMLStreamReader reader =
          XMLInputFactory.newInstance().createXMLStreamReader(new
              ByteArrayInputStream(document.getBytes()));
      String propertyName = "";
      String propertyValue = "";
      String currentElement = "";
           
      
      while (reader.hasNext()) {
        int code = reader.next();
      
        switch (code) {
          case XMLStreamConstants.START_ELEMENT: //START_ELEMENT:
            currentElement = reader.getLocalName();  // Node name
                   
            break;
          case XMLStreamConstants.CHARACTERS:  //CHARACTERS:
        	  propertyValue = currentElement;   // Node Value
        	  context.write(currentElement, propertyValue)        	       	  ;
        	  	  
           
            break;
        }
      }
      reader.close();

     
    }
        catch(Exception e){
                throw new IOException(e);

                }

  }
}
public static class Reduce
    extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void setup(
      Context context)
      throws IOException, InterruptedException {
    context.write(new Text("<configuration>"), null);
  }

  @Override
  protected void cleanup(
      Context context)
      throws IOException, InterruptedException {
    context.write(new Text("</configuration>"), null);
  }

  private Text outputKey = new Text();
  public void reduce(Text key, Iterable values, Context context)
          throws IOException, InterruptedException
      {
          String keyprevious = "";
          context.write(new Text("<property>"), null);
          for(Iterator iterator = values.iterator(); iterator.hasNext();)
          {
              Text value = (Text)iterator.next();
              outputKey.set(constructPropertyXml(key, value));
                         
              keyprevious = key.toString();
          }

          context.write(new Text("</property>"), null);
      }

  public static String constructPropertyXml(Text name, Text value) {
	  StringBuilder sb = new StringBuilder();
      String valu = value.toString();
      
        
          sb.append("<").append(name).append(">").append(value).append("</").append(name).append(">");
     
      return sb.toString();
  }
}



        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);

                conf.set("xmlinput.start", "<?xml");
                conf.set("xmlinput.end", "</clinical_study>");
                Job job = new Job(conf);
                job.setJarByClass(XMLParser.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(XMLParser.Map.class);
                job.setReducerClass(XMLParser.Reduce.class);

                job.setInputFormatClass(XmlInputFormat1.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                Path outputfile = new Path("pramodgupta/thesis/xmlout1");
                  

                FileInputFormat.addInputPath(job, new Path("pramodgupta/thesis/xmlinput1"));
                FileOutputFormat.setOutputPath(job, outputfile);

                job.waitForCompletion(true);
        }
}

public class WriteXMLFile {
	 
	public static void main(String argv[]) {
 
	  try {
 
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
 
		// root elements
		Document doc = docBuilder.newDocument();
		Element rootElement = doc.createElement("company");
		doc.appendChild(rootElement);
 
		// staff elements
		Element staff = doc.createElement("Staff");
		rootElement.appendChild(staff);
 
		// set attribute to staff element
		Attr attr = doc.createAttribute("id");
		attr.setValue("1");
		staff.setAttributeNode(attr);
 
		// shorten way
		// staff.setAttribute("id", "1");
 
		// firstname elements
		Element firstname = doc.createElement("firstname");
		firstname.appendChild(doc.createTextNode("yong"));
		staff.appendChild(firstname);
 
		// lastname elements
		Element lastname = doc.createElement("lastname");
		lastname.appendChild(doc.createTextNode("mook kim"));
		staff.appendChild(lastname);
 
		// nickname elements
		Element nickname = doc.createElement("nickname");
		nickname.appendChild(doc.createTextNode("mkyong"));
		staff.appendChild(nickname);
 
		// salary elements
		Element salary = doc.createElement("salary");
		salary.appendChild(doc.createTextNode("100000"));
		staff.appendChild(salary);
 
		// write the content into xml file
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File("C:\\file.xml"));
 
		// Output to console for testing
		// StreamResult result = new StreamResult(System.out);
 
		transformer.transform(source, result);
 
		System.out.println("File saved!");
 
	  } catch (ParserConfigurationException pce) {
		pce.printStackTrace();
	  } catch (TransformerException tfe) {
		tfe.printStackTrace();
	  }
	}
}