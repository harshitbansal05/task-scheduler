package sa.com.barraq.taskScheduler.model.job.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.jcsp.lang.One2OneChannel;
import sa.com.barraq.taskScheduler.model.job.InternalJob;

import java.util.UUID;

@Getter
@Builder
@AllArgsConstructor
public class JobOutRequest {
    private UUID id;
    private final One2OneChannel<InternalJob> outChan;
}
