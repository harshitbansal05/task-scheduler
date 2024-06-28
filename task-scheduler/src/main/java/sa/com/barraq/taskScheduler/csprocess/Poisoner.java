package sa.com.barraq.taskScheduler.csprocess;

import org.jcsp.lang.CSProcess;
import org.jcsp.lang.ChannelOutput;

public class Poisoner implements CSProcess {
    private final ChannelOutput<Object> out;
    private final int poison;
    private final Object lock;

    public Poisoner(ChannelOutput<Object> out, int poison) {
        this.out = out;
        this.poison = poison;
        this.lock = new Object();
    }

    @Override
    public void run() {
        synchronized (lock) {
            out.poison(poison);
        }
    }
}
