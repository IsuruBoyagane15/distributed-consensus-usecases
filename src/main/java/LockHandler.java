import dcf.ConsensusApplication;
import dcf.DistributedConsensusFramework;
import org.graalvm.polyglot.Value;

public class LockHandler extends ConsensusApplication {
    public LockHandler(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
    }

    @Override
    public boolean onReceiving(Value value) {
        return value.asBoolean();
    }

    @Override
    public void commitAgreedValue(Value value) {
        DistributedConsensusFramework framework = new DistributedConsensusFramework(this);
        for (int i=0; i<10; i++){
            System.out.println(this.getNodeId() + " is holding lock.");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        framework.writeACommand("lockStatuses.delete(\""+ this.getNodeId() + "\"" + ");");
    }

    public static void handleLock(String nodeId, String kafkaServerAddress, String kafkaTopic){
        LockHandler lockHandler = new LockHandler(nodeId, "var lockStatuses = new Set([]); result = false;",
                "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                "if(Array.from(lockStatuses)[0] === \"" + nodeId + "\"){" +
                "result = true;" +
                "}" +
                "result;", kafkaServerAddress, kafkaTopic);

        DistributedConsensusFramework framework = new DistributedConsensusFramework(lockHandler);
        framework.start();
        framework.writeACommand("lockStatuses.add(\""+  lockHandler.getNodeId() + "\"" + ");");
    }

    public static void main(String[] args){
        LockHandler.handleLock(args[0], args[1], args[2]);
    }
}
