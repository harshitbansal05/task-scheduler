package sa.com.barraq.taskScheduler.model.scheduler;

import lombok.*;
import org.jcsp.lang.*;
import org.jcsp.util.Buffer;
import sa.com.barraq.taskScheduler.csprocess.ReceiverFactory;
import sa.com.barraq.taskScheduler.csprocess.SenderFactory;
import sa.com.barraq.taskScheduler.enums.LimitMode;
import sa.com.barraq.taskScheduler.exceptions.ReceiverTimedOutException;
import sa.com.barraq.taskScheduler.exceptions.SenderTimedOutException;
import sa.com.barraq.taskScheduler.model.elector.Elector;
import sa.com.barraq.taskScheduler.model.executor.Executor;
import sa.com.barraq.taskScheduler.model.job.InternalJob;
import sa.com.barraq.taskScheduler.model.job.Job;
import sa.com.barraq.taskScheduler.model.job.definition.JobDefinition;
import sa.com.barraq.taskScheduler.model.job.option.JobOption;
import sa.com.barraq.taskScheduler.model.job.request.*;
import sa.com.barraq.taskScheduler.model.job.request.dto.ReceiverResponse;
import sa.com.barraq.taskScheduler.model.limit.LimitModeConfig;
import sa.com.barraq.taskScheduler.model.locker.Locker;
import sa.com.barraq.taskScheduler.model.scheduler.option.SchedulerOption;
import sa.com.barraq.taskScheduler.utils.TaskSchedulerUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static sa.com.barraq.taskScheduler.exceptions.TaskSchedulerErrors.*;

@Setter
@Builder
@AllArgsConstructor
public class Scheduler implements IScheduler {

    private final Executor executor;
    private List<JobOption> globalJobOptions;
    private final Map<UUID, InternalJob> jobs;
    private boolean started;

    private final One2OneChannel<Object> cancelCh = Channel.one2one(1);
    private final One2OneChannelSymmetric<Object> startCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> startedCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> stopCh = Channel.one2oneSymmetric();
    private final One2OneChannel<Object> stopErrCh = Channel.one2one(new Buffer<>(1), 1);

    private final One2OneChannelSymmetric<Object> allJobsOutRequestCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> jobOutRequestCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> runJobRequestCh = Channel.one2oneSymmetric();

    private final One2OneChannelSymmetric<Object> newJobCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> removeJobCh = Channel.one2oneSymmetric();
    private final One2OneChannelSymmetric<Object> removeJobByTagsCh = Channel.one2oneSymmetric();

    private PriorityQueue<CallBack> q;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public static Scheduler newScheduler(SchedulerOption... options) throws Exception {
        Scheduler scheduler = Scheduler.builder()
                .executor(new Executor())
                .jobs(new HashMap<>())
                .globalJobOptions(new ArrayList<>())
                .build();
        for (SchedulerOption option : List.of(options)) {
            option.apply(scheduler);
        }
        scheduler.q = new PriorityQueue<>((o1, o2) -> (int) (o1.executeAt - o2.executeAt));

        AltingChannelInput<Object>[] functionInputs = new AltingChannelInput[]{
                scheduler.executor.getJobsOutForRescheduling().in(),
                scheduler.executor.getJobsOutCompleted().in(),
                scheduler.newJobCh.in(),
                scheduler.removeJobCh.in(),
                scheduler.removeJobByTagsCh.in(),
                scheduler.executor.getJobOutRequest().in(),
                scheduler.jobOutRequestCh.in(),
                scheduler.runJobRequestCh.in(),
                scheduler.allJobsOutRequestCh.in(),
                scheduler.startCh.in(),
                scheduler.stopCh.in(),
                scheduler.cancelCh.in()
        };

        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    ReceiverResponse response = new ReceiverResponse();
                    ReceiverFactory.getReceiver("scheduler", functionInputs, response).run();
                    applyFunctionCallable(scheduler, response);
                } catch (PoisonException exception) {
                    try {
                        scheduler.stopScheduler();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                } catch (ClassCastException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();

        new Thread(() -> {
            try {
                timer(scheduler);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
        return scheduler;
    }

    private static void timer(Scheduler scheduler) throws InterruptedException {
        while (true) {
            scheduler.lock.lock();
            while (scheduler.q.isEmpty()) {
                scheduler.condition.await();
            }
            while (!scheduler.q.isEmpty()) {
                CallBack callBack = scheduler.q.peek();
                if (!scheduler.condition.await(callBack.executeAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
                    CallBack cb = scheduler.q.poll();
                    scheduler.lock.unlock();
                    execute(scheduler, cb);
                    break;
                }
            }
        }
    }

    private static void execute(Scheduler scheduler, CallBack cb) {
        while (scheduler.started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(scheduler.executor.getJobsIn().hashCode()),
                        scheduler.executor.getJobsIn().out(), new JobIn(cb.getId(), true), 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyFunctionCallable(Scheduler scheduler, ReceiverResponse response) throws InterruptedException {
        switch (response.getReceivedChIndex()) {
            case 0:
                if (response.getData() instanceof UUID) {
                    scheduler.selectExecJobsOutForRescheduling((UUID) response.getData());
                } else throw new ClassCastException();
            case 1:
                if (response.getData() instanceof UUID) {
                    scheduler.selectExecJobsOutCompleted((UUID) response.getData());
                } else throw new ClassCastException();
            case 2:
                if (response.getData() instanceof NewJobIn) {
                    scheduler.selectNewJob((NewJobIn) response.getData());
                } else throw new ClassCastException();
            case 3:
                if (response.getData() instanceof UUID) {
                    scheduler.selectRemoveJob((UUID) response.getData());
                } else throw new ClassCastException();
            case 4:
                if (response.getData() instanceof List<?> list) {
                    for (Object item : list) {
                        if (!(item instanceof String)) {
                            break;
                        }
                    }
                    scheduler.selectRemoveJobsByTags((List<String>) list);
                } else throw new ClassCastException();
            case 5:
                if (response.getData() instanceof JobOutRequest) {
                    scheduler.selectJobOutRequest((JobOutRequest) response.getData());
                } else throw new ClassCastException();
                case 6:
                if (response.getData() instanceof JobOutRequest) {
                    scheduler.selectJobOutRequest((JobOutRequest) response.getData());
                } else throw new ClassCastException();
            case 7:
                if (response.getData() instanceof RunJobRequest) {
                    scheduler.selectRunJobRequest((RunJobRequest) response.getData());
                } else throw new ClassCastException();
            case 8:
                if (response.getData() instanceof AllJobsOutRequest) {
                    scheduler.selectAllJobsOutRequest((AllJobsOutRequest) response.getData());
                } else throw new ClassCastException();
            case 9: scheduler.selectStart();
            case 10: scheduler.stopScheduler();
        }
    }

    private void stopScheduler() throws InterruptedException {
        if (started) {
            SenderFactory.getSender(String.valueOf(executor.getStopCh().hashCode()), executor.getStopCh().out(), new Object()).run();
        }
        for (InternalJob job : jobs.values()) {
            job.stop();
        }
        for (InternalJob job : jobs.values()) {
            try {
                ReceiverFactory.getReceiver(String.valueOf(job.getCancelCh().hashCode()), new AltingChannelInput[]{job.getCancelCh().in()}, new ReceiverResponse());
                // should not have come here, unexpected bug
            } catch (PoisonException e) {
                job.resetCancelCh();
            }
        }
        started = false;
    }

    private void selectRemoveJob(UUID id) {
        if (!jobs.containsKey(id)) return;;
        InternalJob job = jobs.get(id);
        job.stop();
        jobs.remove(id);
    }

    private void selectAllJobsOutRequest(AllJobsOutRequest out) {
        List<Job> outJobs = new ArrayList<>(jobs.size());
        for (InternalJob job : jobs.values()) {
            outJobs.add(jobFromInternalJob(job));
        }
        outJobs.sort(Comparator.comparing(Job::getId));

        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(out.getOutChan().hashCode()),
                        out.getOutChan().out(), outJobs, 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    private void selectRunJobRequest(RunJobRequest run) {
        if (!jobs.containsKey(run.getId())) {
            SenderFactory.getSender(String.valueOf(run.getOutChan().hashCode()), run.getOutChan().out(), ErrJobNotFound).run();
            return;
        }
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(executor.getJobsIn().hashCode()),
                        executor.getJobsIn().out(), new JobIn(run.getId(), false), 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
        Throwable error = !started ? ErrJobRunNowFailed : null;
        SenderFactory.getSender(String.valueOf(run.getOutChan().hashCode()), run.getOutChan().out(), error).run();
    }

    private void selectExecJobsOutForRescheduling(UUID id) {
        if (!jobs.containsKey(id)) return;
        InternalJob job = jobs.get(id);

        LocalDateTime scheduleFrom = null;
        if (!job.getNextScheduled().isEmpty()) {
            // always grab the last element in the slice as that is the furthest
            // out in the future and the time from which we want to calculate
            // the subsequent next run time.
            job.getNextScheduled().sort(LocalDateTime::compareTo);
            scheduleFrom = job.getNextScheduled().getLast();
        }

        LocalDateTime next = job.getJobSchedule().next(scheduleFrom);
        if (next == null) {
            // the job's next function will return zero for OneTime jobs.
            // since they are one time only, they do not need rescheduling.
            return;
        }
        if (next.isBefore(LocalDateTime.now())) {
            // in some cases the next run time can be in the past, for example:
            // - the time on the machine was incorrect and has been synced with ntp
            // - the machine went to sleep, and woke up some time later
            // in those cases, we want to increment to the next run in the future
            // and schedule the job for that time.
            while (next.isBefore(LocalDateTime.now())) next = job.getJobSchedule().next(next);
        }
        job.getNextScheduled().add(next);
        lock.lock();
        q.add(new CallBack(next.toInstant(ZoneOffset.UTC).toEpochMilli(), job.getId()));
        condition.signal();
        lock.unlock();
    }

    private void selectExecJobsOutCompleted(UUID id) {
        if (!jobs.containsKey(id)) return;
        InternalJob job = jobs.get(id);
        if (job.getNextScheduled().size() > 1) {
            List<LocalDateTime> newNextScheduled = new ArrayList<>();
            for (LocalDateTime time : job.getNextScheduled()) {
                if (time.isBefore(LocalDateTime.now())) continue;
                newNextScheduled.add(time);
            }
            job.setNextScheduled(newNextScheduled);
        }

        if (job.getLimitRunsTo() != 0) {
            job.setRunCount(job.getRunCount() + 1);
            if (job.getRunCount() == job.getLimitRunsTo()) {
                new Thread(() -> {
                    while (started) {
                        try {
                            SenderFactory.getSenderWithTimeout(String.valueOf(removeJobCh.hashCode()), removeJobCh.out(), id, 1000).run();
                            break;
                        } catch (SenderTimedOutException e) {
                            // swallow exception
                        }
                    }
                }).start();
                return;
            }
        }
        job.setLastRun(LocalDateTime.now());
    }

    private void selectJobOutRequest(JobOutRequest out) {
        if (!jobs.containsKey(out.getId())) return;
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(out.getOutChan().hashCode()),
                        out.getOutChan().out(), jobs.get(out.getId()), 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    private void selectNewJob(NewJobIn in) {
        InternalJob job = in.getJob();
        if (started) {
            LocalDateTime next = job.getStartTime();
            if (job.isStartImmediately()) {
                next = LocalDateTime.now();
                while (started) {
                    try {
                        SenderFactory.getSenderWithTimeout(String.valueOf(executor.getJobsIn().hashCode()),
                                executor.getJobsIn().out(), new JobIn(job.getId(), true), 1000).run();
                        break;
                    } catch (SenderTimedOutException e) {
                        // swallow exception
                    }
                }
            } else {
                if (next == null) next = job.getJobSchedule().next(LocalDateTime.now());
                lock.lock();
                q.add(new CallBack(next.toInstant(ZoneOffset.UTC).toEpochMilli(), job.getId()));
                condition.signal();
                lock.unlock();
            }
            job.getNextScheduled().add(next);
        }
    }

    private void selectRemoveJobsByTags(List<String> tags) {
        Iterator<Map.Entry<UUID, InternalJob>> iterator = jobs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<UUID, InternalJob> entry = iterator.next();
            InternalJob job = entry.getValue();
            for (String tag : tags) {
                if (job.getTags().contains(tag)) {
                    job.stop();
                    iterator.remove();
                    break;
                }
            }
        }
    }

    @SneakyThrows
    private void selectStart() {
        new Thread(() -> {
            try {
                executor.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();
        started = true;
        for (Map.Entry<UUID, InternalJob> j : jobs.entrySet()) {
            UUID id = j.getKey();
            InternalJob job = j.getValue();
            LocalDateTime next = job.getStartTime();
            if (job.isStartImmediately()) {
                next = LocalDateTime.now();
                while (started) {
                    try {
                        SenderFactory.getSenderWithTimeout(String.valueOf(executor.getJobsIn().hashCode()),
                                executor.getJobsIn().out(), new JobIn(id, true), 1000).run();
                        break;
                    } catch (SenderTimedOutException e) {
                        // swallow exception
                    }
                }
            } else {
                if (next == null) next = job.getJobSchedule().next(LocalDateTime.now());
                lock.lock();
                q.add(new CallBack(next.toInstant(ZoneOffset.UTC).toEpochMilli(), id));
                condition.signal();
                lock.unlock();
            }
            job.getNextScheduled().add(next);
        }
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(startedCh.hashCode()), startedCh.out(), new Object(), 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    private Job jobFromInternalJob(InternalJob job) {
        return Job.builder()
                .id(job.getId())
                .name(job.getName())
                .tags(List.copyOf(job.getTags()))
                .jobOutCh(jobOutRequestCh)
                .runJobCh(runJobRequestCh)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Job> jobs() {
        One2OneChannelSymmetric<Object> resp = Channel.one2oneSymmetric();
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(allJobsOutRequestCh.hashCode()),
                        allJobsOutRequestCh.out(), new AllJobsOutRequest(resp), 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
        while (started) {
            try {
                ReceiverResponse response = new ReceiverResponse();
                ReceiverFactory.getReceiverWithTimeout(String.valueOf(resp.hashCode()), new AltingChannelInput[]{resp.in()}, response, 1000).run();
                return (List<Job>) response.getData();
            } catch (ReceiverTimedOutException e) {
                // swallow exception
            }
        }
        return new ArrayList<>();
    }

    private Job addOrUpdateJob(UUID id, JobDefinition definition,
                               InternalJob.TaskFunction taskFunction,
                               JobOption... jobOptions) throws Exception {
        InternalJob job = new InternalJob();
        if (id == null) {
            job.setId(UUID.randomUUID());
        } else {
            while (started) {
                try {
                    InternalJob currentJob = TaskSchedulerUtils.requestJobWithTimeout(id, jobOutRequestCh, 1000);
                    if (currentJob != null && currentJob.getId() != null) {
                        while (started) {
                            try {
                                SenderFactory.getSenderWithTimeout(String.valueOf(removeJobCh.hashCode()), removeJobCh.out(), id, 1000).run();
                                break;
                            } catch (SenderTimedOutException e) {
                                // swallow exception
                            }
                        }
                    }
                    job.setId(id);
                    break;
                } catch (SenderTimedOutException | ReceiverTimedOutException e) {
                    // swallow exception
                }
            }
        }
        if (taskFunction == null) throw ErrNewJobTaskNil;
        job.setFunction(taskFunction.getTask().getFunction());
        job.setParameters(taskFunction.getTask().getParameters());

        for (JobOption option : globalJobOptions) option.apply(job);
        for (JobOption option : jobOptions) option.apply(job);
        definition.setup(job);
        while (started) {
            try {
                One2OneChannel<Object> cancelCh = Channel.one2one(new Buffer<>(1));
                SenderFactory.getSenderWithTimeout(String.valueOf(newJobCh.hashCode()), newJobCh.out(), new NewJobIn(job, cancelCh), 1000).run();
                while (started) {
                    try {
                        ReceiverFactory.getReceiverWithTimeout(String.valueOf(cancelCh.hashCode()), new AltingChannelInput[]{cancelCh.in()}, new ReceiverResponse(), 1000).run();
                        break;
                    } catch (ReceiverTimedOutException e) {
                        // swallow exception
                    }
                }
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
        return Job.builder()
                .id(job.getId())
                .name(job.getName())
                .tags(job.getTags())
                .jobOutCh(jobOutRequestCh)
                .runJobCh(runJobRequestCh)
                .build();
    }

    @Override
    public Job newJob(JobDefinition definition, InternalJob.TaskFunction taskFunction, JobOption... options) throws Exception {
        return addOrUpdateJob(null, definition, taskFunction, options);
    }

    @Override
    public void removeByTags(String... tags) {
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(removeJobByTagsCh.hashCode()), removeJobByTagsCh.out(), tags, 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    @Override
    public void removeJob(UUID id) throws Exception {
        InternalJob job;
        while (started) {
            try {
                job = TaskSchedulerUtils.requestJobWithTimeout(id, jobOutRequestCh, 1000);
                if (job == null || job.getId() == null) throw ErrJobNotFound;
                break;
            } catch (SenderTimedOutException | ReceiverTimedOutException e) {
                // swallow exception
            }
        }
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(removeJobCh.hashCode()), removeJobCh.out(), id, 1000).run();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    @Override
    public void start() {
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(startCh.hashCode()), startCh.out(), new Object(), 1000);
                ReceiverResponse resp = new ReceiverResponse();
                ReceiverFactory.getReceiver(String.valueOf(startedCh.hashCode()), new AltingChannelInput[]{startedCh.in()}, resp);
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    @Override
    public void stopJobs() throws Exception {
        while (started) {
            try {
                SenderFactory.getSenderWithTimeout(String.valueOf(stopCh.hashCode()), stopCh.out(), new Object(), 1000);
                ReceiverResponse resp = new ReceiverResponse();
                try {
                    ReceiverFactory.getReceiverWithTimeout(String.valueOf(stopErrCh.hashCode()), new AltingChannelInput[]{stopErrCh.in()}, resp, 2000);
                } catch (ReceiverTimedOutException e) {
                    throw ErrStopSchedulerTimedOut;
                }
                if (resp.getData() != null) throw (Exception) resp.getData();
                break;
            } catch (SenderTimedOutException e) {
                // swallow exception
            }
        }
    }

    @Override
    public void shutdown() throws Exception {
        SenderFactory.getPoisoner(String.valueOf(cancelCh.hashCode()), cancelCh.out(), 1).run();
        ReceiverResponse resp = new ReceiverResponse();
        try {
            ReceiverFactory.getReceiverWithTimeout(String.valueOf(stopErrCh.hashCode()), new AltingChannelInput[]{stopErrCh.in()}, resp, 2000);
        } catch (ReceiverTimedOutException e) {
            throw ErrStopSchedulerTimedOut;
        }
        if (resp.getData() != null) throw (Exception) resp.getData();
    }

    @Override
    public Job update(UUID id, JobDefinition definition, InternalJob.TaskFunction taskFunction, JobOption... options) throws Exception {
        return addOrUpdateJob(id, definition, taskFunction, options);
    }

    @Override
    public int jobsWaitingInQueue() {
        if (executor.getLimitModeConfig() != null && executor.getLimitModeConfig().getMode().equals(LimitMode.LIMIT_MODE_WAIT)) {
            return executor.getLimitModeConfig().getInSize();
        }
        return 0;
    }

    public static SchedulerOption withDistributedElector(Elector elector) {
        return (scheduler -> {
            if (elector == null) throw ErrWithDistributedElectorNil;
            scheduler.executor.setElector(elector);
        });
    }

    public static SchedulerOption withDistributedLocker(Locker locker) {
        return (scheduler -> {
            if (locker == null) throw ErrWithDistributedLockerNil;
            scheduler.executor.setLocker(locker);
        });
    }

    public static SchedulerOption withGlobalJobOptions(JobOption... jobOptions) {
        return (scheduler -> scheduler.globalJobOptions = List.of(jobOptions));
    }

    public static SchedulerOption withLimitConcurrentJobs(int limit, LimitMode mode) {
        return (scheduler -> {
            if (limit == 0) throw ErrWithLimitConcurrentJobsZero;
            scheduler.executor.setLimitModeConfig(LimitModeConfig.builder()
                    .mode(mode)
                    .limit(limit)
                    .in(Channel.one2one(new Buffer<>(1000)))
                    .singletonJobs(new HashMap<>())
                    .build());
            if (mode == LimitMode.LIMIT_MODE_RESCHEDULE) {
                scheduler.executor.getLimitModeConfig().setRescheduleLimiter(Channel.one2one(new Buffer<>(limit)));
            }
        });
    }

    public static SchedulerOption withStopTimeout(long duration, TemporalUnit unit) {
        return (scheduler -> {
            if (duration <= 0) throw ErrWithStopTimeoutZeroOrNegative;
            scheduler.executor.setStopTimeout(duration);
            scheduler.executor.setStopTimeoutUnit(unit);
        });
    }

    @Getter
    static class CallBack {
        long executeAt;
        UUID id;

        public CallBack(long executeAfter, UUID id) {
            this.id = id;
            this.executeAt = System.currentTimeMillis() + executeAfter * 1000;
        }
    }
}
