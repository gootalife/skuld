hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/JarFinder.java
            File tempJar = File.createTempFile("hadoop-", "", testDir);
            tempJar = new File(tempJar.getAbsolutePath() + ".jar");
            createJar(baseDir, tempJar);
            tempJar.deleteOnExit();
            return tempJar.getAbsolutePath();
          }
        }

