import distributedConsensus.ConsensusApplication;
import distributedConsensus.DistributedConsensus;

import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class LeaderCandidate extends ConsensusApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderCandidate.class);
    private Thread listeningThread;
    private String electedLeader;
    private int instanceCount;

    public LeaderCandidate(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress,
                           String kafkaTopic, int instanceCount) {
        super(nodeId, runtimeJsCode, evaluationJsCode, kafkaServerAddress, kafkaTopic);
        this.instanceCount = instanceCount;
        this.electedLeader = null;
        this.listeningThread = null;
    }

    @Override
    public boolean onReceiving(Value result) {
        return result.getMember("consensus").asBoolean();
    }

    @Override
    public void commitAgreedValue(Value value) {
        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(this);
        if (value != null){
            System.out.println("Leader is " + value.getMember("value"));
            this.electedLeader = value.getMember("value").asString();
            if (this.electedLeader.equals(getNodeId())){
                consensusFramework.finishRound(getNodeId());
                LOGGER.info("LEADER :" + getNodeId() + " started sending heartbeats.");
                while (true) {
                    System.out.println("ALIVE");
                    consensusFramework.writeACommand("ALIVE," + getNodeId());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        LOGGER.error("LEADER : " + getNodeId() + " is interrupted.");
                        e.printStackTrace();
                    }
                }
            }
            else {
                this.listeningThread = new Thread(new HeartbeatListener(this));
                this.listeningThread.start();
                LOGGER.info("FOLLOWER :" + getNodeId() + " started listening to heartbeats.");
            }
        }
        else {
            if (electedLeader != null){
                if (!electedLeader.equals(getNodeId()) ) {
                    this.listeningThread.interrupt();
                }
            }
        }

    }

    public void setElectedLeader(String electedLeader) {
        this.electedLeader = electedLeader;
    }

    public void setListeningThread(Thread listeningThread) {
        this.listeningThread = listeningThread;
    }

    public static void electLeader(String nodeId, int instanceCount, String kafkaServerAddress, String kafkaTopic){

        LeaderCandidate leaderCandidate = new LeaderCandidate(nodeId, "var nodeRanks = [];result = {consensus:false, value:null};",
"if(Object.keys(nodeRanks).length==" + instanceCount + "){" +
                    "result.consensus=true;" +
                    "var leader = null;"+
                    "var maxRank = 0;"+
                    "for (var i = 0; i < nodeRanks.length; i++) {"+
                        "if(nodeRanks[i].rank > maxRank){"+
                            "result.value = nodeRanks[i].client;" +
                            "maxRank = nodeRanks[i].rank;" +
                        "}" +
                    "}" +
                "}" +
                "result;",
                kafkaServerAddress, kafkaTopic, instanceCount);

        DistributedConsensus consensusFramework = DistributedConsensus.getDistributeConsensus(leaderCandidate);
        consensusFramework.start();

        System.out.println(leaderCandidate.getNodeId());

        int nodeRank = (int)(1 + Math.random()*100);
        System.out.println(nodeRank);
        consensusFramework.writeACommand("nodeRanks.push({client:\""+ nodeId + "\",rank:" +
                nodeRank +"});");
    }

    public void startNewRound(){
        DistributedConsensus dcf = DistributedConsensus.getDistributeConsensus(this);
        this.setRuntimeJsCode("var nodeRanks = [];result = {consensus:false, value:null};");
        this.setElectedLeader(null);
        this.setListeningThread(null);
        this.instanceCount = this.instanceCount - 1;
        this.setEvaluationJsCode("if(Object.keys(nodeRanks).length==" + instanceCount+ "){" +
                "result.consensus=true;" +
                "var leader = null;"+
                "var maxRank = 0;"+
                "for (var i = 0; i < nodeRanks.length; i++) {"+
                "if(nodeRanks[i].rank > maxRank){"+
                "result.value = nodeRanks[i].client;" +
                "maxRank = nodeRanks[i].rank;" +
                "}" +
                "}" +
                "}" +
                "result;");
        dcf.writeACommand("IN,"+ getNodeId());
        int nodeRank = (int)(1 + Math.random()*100);
        System.out.println(nodeRank);
        dcf.writeACommand("nodeRanks.push({client:\""+ getNodeId() + "\",rank:" +
                nodeRank +"});");
        LOGGER.info("New leader election is started.");
    }

    public static void main(String args[]){
        String Id = UUID.randomUUID().toString();
        LeaderCandidate.electLeader(Id, Integer.parseInt(args[0]),args[1], args[2]);
    }
}
