/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.scheduler;

import static sys.scheduler.VT_Scheduler.Scheduler;

/**
 * 
 * This is the class for creating aperiodic tasks.
 * 
 * Typically this class will be used to create anonymous classes by overriding
 * run() to place the code to be executed at given time in the future.
 * 
 * By default, tasks execute only once. They can be re-scheduled (often within
 * run()) to execute again at a later time.
 * 
 * Tasks can be cancelled to prevent them from executing.
 * 
 * 
 * @author smduarte (smd@fct.unl.pt)
 * 
 */
public class Task implements Comparable<Task> {

    public double due;
    private TaskOwner owner;
    protected boolean isQueued;
    protected boolean isCancelled;
    protected boolean wasReScheduled;

    protected int seqN = g_seqN++;
    private static int g_seqN;

    protected double period;

    int queuePosition;

    /**
     * Creates an new Task. By default it executes once, when it is due. Can be
     * re-scheduled to execute again at a given later time.
     * 
     * @param due
     *            Number of seconds to wait until the task executes.
     */
    public Task(double due) {
        this(null, due);
    }

    /**
     * Creates an new Task. By default it executes once, when it is due. Can be
     * re-scheduled to execute again at a given later time.
     * 
     * @param owner
     *            - Owner of the task, a node for certain in this case.
     *            Important for execution in simulation environments.
     * @param due
     *            Number of seconds to wait until the task executes.
     */
    public Task(TaskOwner owner, double due) {
        this.owner = owner;
        Scheduler.schedule(this, due);
        if (owner != null)
            owner.registerTask(this);
    }

    public Task(TaskOwner owner, double due, double period) {
        this.owner = owner;
        this.period = period;

        Scheduler.schedule(this, due);
        if (owner != null)
            owner.registerTask(this);
    }

    /*
     * (non-Javadoc)
     * 
     * This method should be overriden in all concrete subtypes of this base
     * class.
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        System.err.println("Unexpected execution of Task.run() method.");
        System.err.println("Override public void run() in parent class...");
    }

    /**
     * Tells the time when the task is to due to execute.
     * 
     * @return The time when the task is due to execute.
     */
    public double due() {
        return due;
    }

    /**
     * Cancels the tasks, preventing it from being executed again.
     */
    public void cancel() {
        isCancelled = true;
        wasReScheduled = false;
    }

    /**
     * Asks the task to be scheduled again to execute after the given delay. The
     * period is maintained.
     * 
     * @param t
     *            The new deadline for next execution of this task.
     */
    public void reSchedule(double t) {
        Scheduler.reSchedule(this, t);
        wasReScheduled = true;
    }

    /**
     * Tells if the task is scheduled for execution.
     * 
     * @return true if the task is scheduled for execution or false otherwise.
     */
    public boolean isScheduled() {
        return isQueued;
    }

    /**
     * Tells if the task was reScheduled for execution at a different time...
     * 
     * @return true if the task was reScheduled or false otherwise.
     */
    public boolean wasReScheduled() {
        return wasReScheduled;
    }

    protected void reSchedule() {
    }

    protected void reset() {
        // isQueued = false ;
        wasReScheduled = false;
    }

    @Override
    public int hashCode() {
        return seqN;
    }

    @Override
    public boolean equals(Object o) {
        Task other = (Task) o;
        return other != null && seqN == other.seqN;
    }

    @Override
    public int compareTo(Task other) {
        assert other != null;
        if (due == other.due)
            return (seqN - other.seqN);
        else
            return due < other.due ? -1 : 1;
    }

    protected void release() {
        if (token != null)
            token.unblock();
    }

    public void block() {
        token = Scheduler.newToken();
        token.block();
    }

    @Override
    public String toString() {
        return String.format("%d / %f / %s / %s [%s, %s]", seqN, due, (owner == null ? "" : "" + owner.toString()),
                getClass(), isQueued, wasReScheduled);
    }

    private Token token = null;
}
