hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/view/HtmlPage.java

  @Override
  public void render() {
    putWithoutEscapeHtml(DOCTYPE);
    render(page().html().meta_http("X-UA-Compatible", "IE=8")
        .meta_http("Content-type", MimeType.HTML));
    if (page().nestLevel() != 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/view/TextView.java
  }

  public void echo(Object... args) {
    }
  }

  public void echoWithoutEscapeHtml(Object... args) {
    PrintWriter out = writer();
    for (Object s : args) {
      out.print(s);
    }
  }

    echo(args);
    writer().println();
  }

  public void putWithoutEscapeHtml(Object args) {
    echoWithoutEscapeHtml(args);
    writer().println();
  }
}

