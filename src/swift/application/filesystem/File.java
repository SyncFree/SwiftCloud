package swift.application.filesystem;

public interface File {
    void update(String update, int pos);

    String getContent();
}
