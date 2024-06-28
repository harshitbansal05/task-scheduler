package sa.com.barraq.taskScheduler.model.job.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jcsp.lang.One2OneChannelSymmetric;

@Getter
@AllArgsConstructor
public class AllJobsOutRequest {
    One2OneChannelSymmetric<Object> outChan;
}
