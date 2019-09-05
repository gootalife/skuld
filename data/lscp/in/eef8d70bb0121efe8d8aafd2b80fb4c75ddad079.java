hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/MapWritable.java
      instance.put(key, value);
    }
  }

  @Override
  public String toString() {
    return instance.toString();
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/TestMapWritable.java
    assertEquals(map1.hashCode(), map2.hashCode());
    assertFalse(map1.hashCode() == map3.hashCode());
  }

  public void testToString() {
    MapWritable map = new MapWritable();
    final IntWritable key = new IntWritable(5);
    final Text value = new Text("value");
    map.put(key, value);
    assertEquals("{5=value}", map.toString());
  }
}

