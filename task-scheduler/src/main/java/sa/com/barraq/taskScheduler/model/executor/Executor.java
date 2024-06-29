package sa.com.barraq.taskScheduler.model.executor;

import lombok.Getter;
import lombok.Setter;
import org.jcsp.lang.*;
import org.jcsp.util.Buffer;
import sa.com.barraq.taskScheduler.csprocess.ReceiverFactory;
import sa.com.barraq.taskScheduler.csprocess.SenderFactory;
import sa.com.barraq.taskScheduler.enums.LimitMode;
import sa.com.barraq.taskScheduler.exceptions.ReceiverTimedOutException;
import sa.com.barraq.taskScheduler.exceptions.SenderTimedOutException;
import sa.com.barraq.taskScheduler.model.elector.Elector;
import sa.com.barraq.taskScheduler.model.job.InternalJob;
import sa.com.barraq.taskScheduler.model.job.request.JobIn;
import sa.com.barraq.taskScheduler.model.job.request.SingletonRunner;
import sa.com.barraq.taskScheduler.model.job.request.dto.ReceiverResponse;
import sa.com.barraq.taskScheduler.model.limit.LimitModeConfig;
import sa.com.barraq.taskScheduler.model.lock.Lock;
import sa.com.barraq.taskScheduler.model.locker.Locker;
import sa.com.barraq.taskScheduler.utils.TaskSchedulerUtils;

import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static sa.com.barraq.taskScheduler.exceptions.TaskSchedulerErrors.ErrStopJobsTimedOut;

@Getter
@Setter
public class Executor {
    Elector elector;
    Locker locker;
    LimitModeConfig limitModeConfig;
    long stopTimeout;
    TemporalUnit stopTimeoutUnit;
    Map<UUID, SingletonRunner> singletonRunners;
    boolean started;

    private final One2OneChannel<Object> cancelCh = Channel.one2one(1);
    private One2OneChannelSymmetric<Object> stopCh;
    private One2OneChannelSymmetric<Object> jobsIn;
    private One2OneChannelSymmetric<Object> jobsOutForRescheduling;
    private One2OneChannelSymmetric<Object> jobsOutCompleted;
    private One2OneChannelSymmetric<Object> jobOutRequest;
    private One2OneChannelSymmetric<Object> done;

    private final Object singletonJobsNotifier = new Object();
    private final Object standardJobsNotifier = new Object();
    private final Object limitModeJobsNotifier = new Object();

    public void start() throws Exception {
        Semaphore standardJobsSem = new Semaphore(0);
        Semaphore singletonJobsSem = new Semaphore(0);
        Semaphore limitModeJobsSem = new Semaphore(0);

        singletonRunners = new ConcurrentHashMap<>();
        AltingChannelInput<Object>[] functionInputs = new AltingChannelInput[]{jobsIn.in(), stopCh.in()};
        started = true;
        while (true) {
            ReceiverResponse response = new ReceiverResponse();
            ReceiverFactory.getReceiver("executor", functionInputs, response).run();
            if (response.getReceivedChIndex() == 0) handleJobIn((JobIn) response.getData(), standardJobsSem, singletonJobsSem, limitModeJobsSem);
            else {
                stop(standardJobsSem, singletonJobsSem, limitModeJobsSem);
                return;
            }
        }
    }

    private void handleJobIn(JobIn jobIn, Semaphore standardJobsSem, Semaphore singletonJobsSem, Semaphore limitModeJobsSem) throws Exception {
        if (limitModeConfig.getMode() != null || !limitModeConfig.isStarted()) {
            limitModeConfig.setStarted(true);
            for (int i = 0; i < limitModeConfig.getLimit(); i++) {
                limitModeJobsSem.release();
                int finalI = i;
                new Thread(() -> {
                    try {
                        limitModeRunner("limitMode-" + finalI,
                                limitModeConfig.getIn(),
                                limitModeJobsSem,
                                limitModeConfig.getMode(),
                                limitModeConfig.getRescheduleLimiter());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                ).start();
            }

            new Thread(() -> {
                if (limitModeConfig.getMode() != null) {
                    if (limitModeConfig.getMode().equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                        limitModeConfig.getLock().lock();
                        if (limitModeConfig.getRescheduleLimiterSize() == limitModeConfig.getLimit()) {
                            limitModeConfig.getLock().unlock();
                            sendOutForRescheduling(jobIn);
                        } else {
                            limitModeConfig.setRescheduleLimiterSize(limitModeConfig.getRescheduleLimiterSize() + 1);
                            limitModeConfig.getLock().unlock();
                            SenderFactory.getSender(String.valueOf(limitModeConfig.getRescheduleLimiter().hashCode()),
                                    limitModeConfig.getRescheduleLimiter().out(), jobIn).run();
                            sendOutForRescheduling(jobIn);
                        }
                    } else {
                        sendOutForRescheduling(jobIn);
                        SenderFactory.getSender(String.valueOf(limitModeConfig.getRescheduleLimiter().hashCode()),
                                limitModeConfig.getRescheduleLimiter().out(), jobIn).run();
                    }
                } else {
                    InternalJob job = TaskSchedulerUtils.requestJobWithGuards(jobIn.getId(), new AltingChannelInput[]{cancelCh.in()}, jobOutRequest);
                    if (job == null) return;
                    if (job.isSingletonMode()) {
                        SingletonRunner runner;
                        if (singletonRunners.containsKey(job.getId())) {
                            runner = singletonRunners.get(job.getId());
                        } else {
                            runner = new SingletonRunner();
                            runner.setIn(Channel.one2one(new Buffer<>(1000)));
                            if (job.getSingletonLimitMode().equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                                runner.setRescheduleLimiter(Channel.one2one(new Buffer<>(1)));
                            }
                            singletonRunners.put(job.getId(), runner);
                            singletonJobsSem.release();
                            new Thread(() -> {
                                try {
                                    singletonModeRunner("singleton-" + job.getId(),
                                            runner.getIn(),
                                            singletonJobsSem,
                                            job.getSingletonLimitMode(),
                                            runner.getRescheduleLimiter());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            ).start();
                        }
                        if (job.getSingletonLimitMode().equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                            runner.getLock().lock();
                            if (runner.getRescheduleLimiterSize() == 1) {
                                limitModeConfig.getLock().unlock();
                                sendOutForRescheduling(jobIn);
                            } else {
                                runner.setRescheduleLimiterSize(runner.getRescheduleLimiterSize() + 1);
                                limitModeConfig.getLock().unlock();
                                SenderFactory.getSender(String.valueOf(runner.getRescheduleLimiter().hashCode()),
                                        runner.getRescheduleLimiter().out(), jobIn).run();
                                sendOutForRescheduling(jobIn);
                            }
                        } else {
                            SenderFactory.getSender(String.valueOf(runner.getRescheduleLimiter().hashCode()),
                                    runner.getRescheduleLimiter().out(), jobIn).run();
                            sendOutForRescheduling(jobIn);
                        }
                    } else {
                        standardJobsSem.release();
                        new Thread(() -> {
                            try {
                                runJob(job, jobIn);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            try {
                                singletonJobsSem.acquire();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                    }
                }
            }).start();
        }
    }

    private void sendOutForRescheduling(JobIn jobIn) {
        if (jobIn.isShouldSendOut()) {
            while (started) {
                try {
                    SenderFactory.getSenderWithTimeout(String.valueOf(jobsOutForRescheduling.hashCode()),
                            jobsOutForRescheduling.out(), jobIn.getId(), 1000).run();
                    break;
                } catch (SenderTimedOutException e) {
                    // swallow exception
                }
            }
        }
        jobIn.setShouldSendOut(false);
    }

    private void singletonModeRunner(String s, One2OneChannel<Object> in, Semaphore singletonJobsSem, LimitMode singletonLimitMode, One2OneChannel<Object> rescheduleLimiter) throws Exception {
        while (started) {
            try {
                ReceiverResponse response = new ReceiverResponse();
                ReceiverFactory.getReceiverWithTimeout(String.valueOf(in.hashCode()), new AltingChannelInput[]{in.in()}, response, 1000).run();
                JobIn jobIn = ((JobIn) response.getData());
                InternalJob job = TaskSchedulerUtils.requestJobWithGuards(jobIn.getId(), new AltingChannelInput[]{cancelCh.in()}, jobOutRequest);
                if (job != null) {
                    jobIn.setShouldSendOut(false);
                    runJob(job, jobIn);
                }
                if (singletonLimitMode.equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                    ReceiverFactory.getReceiver(String.valueOf(rescheduleLimiter.hashCode()), new AltingChannelInput[]{rescheduleLimiter.in()}, new ReceiverResponse());
                }
            } catch (ReceiverTimedOutException e) {
                // swallow exception
            }
        }
        singletonJobsSem.acquire();
    }

    private void limitModeRunner(String s, One2OneChannel<Object> in, Semaphore limitModeJobsSem, LimitMode mode, One2OneChannel<Object> rescheduleLimiter) throws Exception {
        while (started) {
            try {
                ReceiverResponse response = new ReceiverResponse();
                ReceiverFactory.getReceiverWithTimeout(String.valueOf(in.hashCode()), new AltingChannelInput[]{in.in()}, response, 1000).run();
                JobIn jobIn = ((JobIn) response.getData());
                InternalJob job = TaskSchedulerUtils.requestJobWithGuards(jobIn.getId(), new AltingChannelInput[]{cancelCh.in()}, jobOutRequest);
                if (job != null) {
                    if (job.isSingletonMode()) {
                        limitModeConfig.getLock().lock();
                        if (limitModeConfig.getSingletonJobs().containsKey(jobIn.getId())) {
                            // this job is already running, so don't run it
                            // but instead reschedule it
                            limitModeConfig.getLock().unlock();
                            if (jobIn.isShouldSendOut()) {
                                SenderFactory.getSenderWithGuards(String.valueOf(jobsOutForRescheduling.hashCode()),
                                        jobsOutForRescheduling.out(),
                                        new AltingChannelInput[]{cancelCh.in()},
                                        jobIn.getId());
                            }

                            if (mode.equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                                ReceiverFactory.getReceiver(String.valueOf(rescheduleLimiter.hashCode()), new AltingChannelInput[]{rescheduleLimiter.in()}, new ReceiverResponse());
                            }
                            continue;
                        }
                        limitModeConfig.getSingletonJobs().put(jobIn.getId(), new Object());
                        limitModeConfig.getLock().unlock();
                    }
                    runJob(job, jobIn);
                    if (job.isSingletonMode()) {
                        limitModeConfig.getLock().lock();
                        limitModeConfig.getSingletonJobs().remove(jobIn.getId());
                        limitModeConfig.getLock().unlock();
                    }
                }
                if (mode.equals(LimitMode.LIMIT_MODE_RESCHEDULE)) {
                    ReceiverFactory.getReceiver(String.valueOf(rescheduleLimiter.hashCode()), new AltingChannelInput[]{rescheduleLimiter.in()}, new ReceiverResponse());
                }
            } catch (ReceiverTimedOutException e) {
                // swallow exception
            }
        }
        limitModeJobsSem.acquire();
    }

    private void runJob(InternalJob job, JobIn jobIn) throws Exception {
        Lock lock = null;
        if (elector != null) {
            try {
                elector.isLeader();
            } catch (Exception e) {
                sendOutForRescheduling(jobIn);
                return;
            }
        } else if (job.getLocker() != null) {
            try {
                lock = job.getLocker().lock(job.getName());
            } catch (Exception e) {
                sendOutForRescheduling(jobIn);
                return;
            }
        } else if (locker != null) {
            try {
                lock = locker.lock(job.getName());
            } catch (Exception e) {
                sendOutForRescheduling(jobIn);
                return;
            }
        }
        try {
            TaskSchedulerUtils.executeJobFunction(job.getBeforeJobRuns(), job.getId(), job.getName());
        } catch (Exception e) {
            TaskSchedulerUtils.executeJobFunction(job.getAfterJobRunsWithError(), job.getId(), job.getName());
            if (lock != null) lock.unlock();
            return;
        }
        TaskSchedulerUtils.executeJobFunction(job.getAfterJobRuns(), job.getId(), job.getName());
        if (lock != null) lock.unlock();
    }

    private void stop(Semaphore standardJobsSem, Semaphore singletonJobsSem, Semaphore limitModeJobsSem) throws Exception {
        SenderFactory.getPoisoner(String.valueOf(cancelCh.hashCode()), cancelCh.out(), 1);
        CountDownLatch latch = new CountDownLatch(3);

        new Thread(() -> {
            synchronized (singletonJobsNotifier) {
                while (singletonJobsSem.availablePermits() > 0) {
                    try {
                        singletonJobsNotifier.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            latch.countDown();
        }).start();
        new Thread(() -> {
            synchronized (standardJobsNotifier) {
                while (standardJobsSem.availablePermits() > 0) {
                    try {
                        standardJobsNotifier.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            latch.countDown();
        }).start();
        new Thread(() -> {
            synchronized (limitModeJobsNotifier) {
                while (limitModeJobsSem.availablePermits() > 0) {
                    try {
                        limitModeJobsNotifier.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            latch.countDown();
        }).start();

        boolean stopped = latch.await(stopTimeout, TimeUnit.MILLISECONDS);
        if (!stopped) throw ErrStopJobsTimedOut;
    }
}
