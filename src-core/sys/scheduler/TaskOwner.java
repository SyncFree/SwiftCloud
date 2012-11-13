package sys.scheduler;

public interface TaskOwner {

	public void registerTask(Task t);

	public void cancelAllTasks();
}
