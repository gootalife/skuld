hadoop-maven-plugins/src/main/java/org/apache/hadoop/maven/plugin/protoc/ProtocMojo.java
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

@Mojo(name="protoc", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ProtocMojo extends AbstractMojo {
  @Parameter(required=true)
  private String protocVersion;

  @Parameter(defaultValue =
      "${project.build.directory}/hadoop-maven-plugins-protoc-checksums.json")
  private String checksumPath;

  public class ChecksumComparator {

    private final Map<String, Long> storedChecksums;
    private final Map<String, Long> computedChecksums;

    private final File checksumFile;

    ChecksumComparator(String checksumPath) throws IOException {
      checksumFile = new File(checksumPath);
      if (checksumFile.exists()) {
        ObjectMapper mapper = new ObjectMapper();
        storedChecksums = mapper
            .readValue(checksumFile, new TypeReference<Map<String, Long>>() {
            });
      } else {
        storedChecksums = new HashMap<>(0);
      }
      computedChecksums = new HashMap<>();
    }

    public boolean hasChanged(File file) throws IOException {
      if (!file.exists()) {
        throw new FileNotFoundException(
            "Specified protoc include or source does not exist: " + file);
      }
      if (file.isDirectory()) {
        return hasDirectoryChanged(file);
      } else if (file.isFile()) {
        return hasFileChanged(file);
      } else {
        throw new IOException("Not a file or directory: " + file);
      }
    }

    private boolean hasDirectoryChanged(File directory) throws IOException {
      File[] listing = directory.listFiles();
      boolean changed = false;
      for (File f : listing) {
        if (f.isDirectory()) {
          if (hasDirectoryChanged(f)) {
            changed = true;
          }
        } else if (f.isFile()) {
          if (hasFileChanged(f)) {
            changed = true;
          }
        } else {
          getLog().debug("Skipping entry that is not a file or directory: "
              + f);
        }
      }
      return changed;
    }

    private boolean hasFileChanged(File file) throws IOException {
      long computedCsum = computeChecksum(file);

      Long storedCsum = storedChecksums.get(file.getCanonicalPath());
      if (storedCsum == null || storedCsum.longValue() != computedCsum) {
        return true;
      }
      return false;
    }

    private long computeChecksum(File file) throws IOException {
      final String canonicalPath = file.getCanonicalPath();
      if (computedChecksums.containsKey(canonicalPath)) {
        return computedChecksums.get(canonicalPath);
      }
      CRC32 crc = new CRC32();
      byte[] buffer = new byte[1024*64];
      try (BufferedInputStream in =
          new BufferedInputStream(new FileInputStream(file))) {
        while (true) {
          int read = in.read(buffer);
          if (read <= 0) {
            break;
          }
          crc.update(buffer, 0, read);
        }
      }
      final long computedCsum = crc.getValue();
      computedChecksums.put(canonicalPath, computedCsum);
      return crc.getValue();
    }

    public void writeChecksums() throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      try (BufferedOutputStream out = new BufferedOutputStream(
          new FileOutputStream(checksumFile))) {
        mapper.writeValue(out, computedChecksums);
        getLog().info("Wrote protoc checksums to file " + checksumFile);
      }
    }
  }

  public void execute() throws MojoExecutionException {
    try {
      List<String> command = new ArrayList<String>();
      }
      if (!output.mkdirs()) {
        if (!output.exists()) {
          throw new MojoExecutionException(
              "Could not create directory: " + output);
        }
      }

      ChecksumComparator comparator = new ChecksumComparator(checksumPath);
      boolean importsChanged = false;

      command = new ArrayList<String>();
      command.add(protocCommand);
      command.add("--java_out=" + output.getCanonicalPath());
      if (imports != null) {
        for (File i : imports) {
          if (comparator.hasChanged(i)) {
            importsChanged = true;
          }
          command.add("-I" + i.getCanonicalPath());
        }
      }
      List<File> changedSources = new ArrayList<>();
      boolean sourcesChanged = false;
      for (File f : FileSetUtils.convertFileSetToFiles(source)) {
        if (comparator.hasChanged(f) || importsChanged) {
          sourcesChanged = true;
          changedSources.add(f);
          command.add(f.getCanonicalPath());
        }
      }

      if (!sourcesChanged && !importsChanged) {
        getLog().info("No changes detected in protoc files, skipping "
            + "generation.");
      } else {
        if (getLog().isDebugEnabled()) {
          StringBuilder b = new StringBuilder();
          b.append("Generating classes for the following protoc files: [");
          String prefix = "";
          for (File f : changedSources) {
            b.append(prefix);
            b.append(f.toString());
            prefix = ", ";
          }
          b.append("]");
          getLog().debug(b.toString());
        }

        exec = new Exec(this);
        out = new ArrayList<String>();
        if (exec.run(command, out) != 0) {
          }
          throw new MojoExecutionException("protoc failure");
        }
        comparator.writeChecksums();
      }
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }

