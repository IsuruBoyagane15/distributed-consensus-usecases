import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatListener extends Thread {
    private LeaderCandidate follower;

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);

    public HeartbeatListener(LeaderCandidate follower){
        this.follower = follower;
    }

    public void run() {
        try {
            Thread.sleep(2000);
            System.out.println("Original leader failed.");
            LOGGER.info("Leader failed.");
            follower.startNewRound();
        } catch (InterruptedException e) {
            System.out.println("Leader sent the heartbeat");
            run();
        }
    }
}
