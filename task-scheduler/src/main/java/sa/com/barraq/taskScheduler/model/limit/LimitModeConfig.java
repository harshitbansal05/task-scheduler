package sa.com.barraq.taskScheduler.model.limit;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.jcsp.lang.One2OneChannel;
import sa.com.barraq.taskScheduler.enums.LimitMode;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Setter
@Builder
public class LimitModeConfig {
    private boolean started;
    private LimitMode mode;
    private int limit, rescheduleLimiterSize, inSize;
    private One2OneChannel<Object> rescheduleLimiter;
    private Map<UUID, Object> singletonJobs;
    private One2OneChannel<Object> in;

    private final ReentrantLock lock = new ReentrantLock();
}
