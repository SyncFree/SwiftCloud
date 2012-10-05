package swift.application.filesystem;

public class FileBasic implements File {
    private final StringBuffer content;

    public FileBasic(String s) {
        this.content = new StringBuffer(s);
    }

    public void update(String update, int pos) {
        this.content.insert(pos, update);
    }

    public String getContent() {
        return content.toString();
    }

}
