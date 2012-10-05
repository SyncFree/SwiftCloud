package swift.application.filesystem;

public interface IFile {
    void update(String update, int pos);

    String getContent();

}
