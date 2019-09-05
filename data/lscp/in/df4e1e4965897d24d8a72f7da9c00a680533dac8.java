hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryEventHandler.java

  private int numUnflushedCompletionEvents = 0;
  private boolean isTimerActive;
  private EventWriter.WriteMode jhistMode =
      EventWriter.WriteMode.JSON;

  protected BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();
      LOG.info("Emitting job history data to the timeline server is not enabled");
    }

    String jhistFormat = conf.get(JHAdminConfig.MR_HS_JHIST_FORMAT,
        JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT);
    if (jhistFormat.equals("json")) {
      jhistMode = EventWriter.WriteMode.JSON;
    } else if (jhistFormat.equals("binary")) {
      jhistMode = EventWriter.WriteMode.BINARY;
    } else {
      LOG.warn("Unrecognized value '" + jhistFormat + "' for property " +
          JHAdminConfig.MR_HS_JHIST_FORMAT + ".  Valid values are " +
          "'json' or 'binary'.  Falling back to default value '" +
          JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT + "'.");
    }

    super.serviceInit(conf);
  }

  protected EventWriter createEventWriter(Path historyFilePath)
      throws IOException {
    FSDataOutputStream out = stagingDirFS.create(historyFilePath, true);
    return new EventWriter(out, this.jhistMode);
  }
  

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/jobhistory/TestEvents.java
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FSDataOutputStream fsOutput = new FSDataOutputStream(output,
        new FileSystem.Statistics("scheme"));
    EventWriter writer = new EventWriter(fsOutput,
        EventWriter.WriteMode.JSON);
    writer.write(getJobPriorityChangedEvent());
    writer.write(getJobStatusChangedEvent());
    writer.write(getTaskUpdatedEvent());

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-common/src/main/java/org/apache/hadoop/mapreduce/v2/jobhistory/JHAdminConfig.java
      + "jobname.limit";
  public static final int DEFAULT_MR_HS_JOBNAME_LIMIT = 50;

  public static final String MR_HS_JHIST_FORMAT =
      MR_HISTORY_PREFIX + "jhist.format";
  public static final String DEFAULT_MR_HS_JHIST_FORMAT =
      "json";
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventReader.java
    this.in = in;
    this.version = in.readLine();

    Schema myschema = new SpecificData(Event.class.getClassLoader()).getSchema(Event.class);
    Schema.Parser parser = new Schema.Parser();
    this.schema = parser.parse(in.readLine());
    this.reader = new SpecificDatumReader(schema, myschema);
    if (EventWriter.VERSION.equals(version)) {
      this.decoder = DecoderFactory.get().jsonDecoder(schema, in);
    } else if (EventWriter.VERSION_BINARY.equals(version)) {
      this.decoder = DecoderFactory.get().binaryDecoder(in, null);
    } else {
      throw new IOException("Incompatible event log version: " + version);
    }
  }
  

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventWriter.java
class EventWriter {
  static final String VERSION = "Avro-Json";
  static final String VERSION_BINARY = "Avro-Binary";

  private FSDataOutputStream out;
  private DatumWriter<Event> writer =
    new SpecificDatumWriter<Event>(Event.class);
  private Encoder encoder;
  private static final Log LOG = LogFactory.getLog(EventWriter.class);
  public enum WriteMode { JSON, BINARY }
  private final WriteMode writeMode;
  private final boolean jsonOutput;  // Cache value while we have 2 modes

  EventWriter(FSDataOutputStream out, WriteMode mode) throws IOException {
    this.out = out;
    this.writeMode = mode;
    if (this.writeMode==WriteMode.JSON) {
      this.jsonOutput = true;
      out.writeBytes(VERSION);
    } else if (this.writeMode==WriteMode.BINARY) {
      this.jsonOutput = false;
      out.writeBytes(VERSION_BINARY);
    } else {
      throw new IOException("Unknown mode: " + mode);
    }
    out.writeBytes("\n");
    out.writeBytes(Event.SCHEMA$.toString());
    out.writeBytes("\n");
    if (!this.jsonOutput) {
      this.encoder = EncoderFactory.get().binaryEncoder(out, null);
    } else {
      this.encoder = EncoderFactory.get().jsonEncoder(Event.SCHEMA$, out);
    }
  }
  
  synchronized void write(HistoryEvent event) throws IOException { 
    Event wrapper = new Event();
    wrapper.setEvent(event.getDatum());
    writer.write(wrapper, encoder);
    encoder.flush();
    if (this.jsonOutput) {
      out.writeBytes("\n");
    }
  }
  
  void flush() throws IOException {
    encoder.flush();

