package fogcomputing.titanic;

import java.util.Formatter;

import fogcomputing.broker.MDP;
import org.zeromq.*;

public class mdwrkapi
{

    private static final int HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable

    private String   broker;
    private ZContext ctx;
    private String   service;

    private ZMQ.Socket worker;
    private long       heartbeatAt;
    private int        liveness;
    private int        heartbeat = 2500;
    private int        reconnect = 2500;

    private boolean expectReply = false;
    private long      timeout = 2500;
    private boolean   verbose;
    private Formatter log     = new Formatter(System.out);

    // Return address, if any
    private ZFrame replyTo;

    public mdwrkapi(String broker, String service, boolean verbose)
    {
        assert (broker != null);
        assert (service != null);
        this.broker = broker;
        this.service = service;
        this.verbose = verbose;
        ctx = new ZContext();
        reconnectToBroker();
    }

    void sendToBroker(MDP command, String option, ZMsg msg)
    {
        msg = msg != null ? msg.duplicate() : new ZMsg();


        if (option != null)
            msg.addFirst(new ZFrame(option));

        msg.addFirst(command.newFrame());
        msg.addFirst(MDP.W_WORKER.newFrame());
        msg.addFirst(new ZFrame(ZMQ.MESSAGE_SEPARATOR));

        if (verbose) {
            log.format("I: sending %s to broker\n", command);
            msg.dump(log.out());
        }
        msg.send(worker);
    }

    void reconnectToBroker()
    {
        if (worker != null) {
            ctx.destroySocket(worker);
        }
        worker = ctx.createSocket(ZMQ.DEALER);
        worker.connect(broker);
        if (verbose)
            log.format("I: connecting to broker at %s\n", broker);


        sendToBroker(MDP.W_READY, service, null);

        liveness = HEARTBEAT_LIVENESS;
        heartbeatAt = System.currentTimeMillis() + heartbeat;

    }

    public ZMsg receive(ZMsg reply)
    {

        assert (reply != null || !expectReply);

        if (reply != null) {
            assert (replyTo != null);
            reply.wrap(replyTo);
            sendToBroker(MDP.W_REPLY, null, reply);
            reply.destroy();
        }
        expectReply = true;

        while (!Thread.currentThread().isInterrupted()) {

            ZMQ.Poller items = ctx.createPoller(1);
            items.register(worker, ZMQ.Poller.POLLIN);
            if (items.poll(timeout) == -1)
                break;

            if (items.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(worker);
                if (msg == null)
                    break;
                if (verbose) {
                    log.format("I: received message from broker: \n");
                    msg.dump(log.out());
                }
                liveness = HEARTBEAT_LIVENESS;
                assert (msg != null && msg.size() >= 3);

                ZFrame empty = msg.pop();
                assert (empty.getData().length == 0);
                empty.destroy();

                ZFrame header = msg.pop();
                assert (MDP.W_WORKER.frameEquals(header));
                header.destroy();

                ZFrame command = msg.pop();
                if (MDP.W_REQUEST.frameEquals(command)) {
                    replyTo = msg.unwrap();
                    command.destroy();
                    return msg;
                }
                else if (MDP.W_HEARTBEAT.frameEquals(command)) {
                }
                else if (MDP.W_DISCONNECT.frameEquals(command)) {
                    reconnectToBroker();
                }
                else {
                    log.format("E: invalid input message: \n");
                    msg.dump(log.out());
                }
                command.destroy();
                msg.destroy();
            }
            else if (--liveness == 0) {
                if (verbose)
                    log.format("W: disconnected from broker - retrying\n");
                try {
                    Thread.sleep(reconnect);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    break;
                }
                reconnectToBroker();

            }

            if (System.currentTimeMillis() > heartbeatAt) {
                sendToBroker(MDP.W_HEARTBEAT, null, null);
                heartbeatAt = System.currentTimeMillis() + heartbeat;
            }
            items.close();
        }
        if (Thread.currentThread().isInterrupted())
            log.format("W: interrupt received, killing worker\n");
        return null;
    }

    public void destroy()
    {
        ctx.destroy();
    }

    public int getHeartbeat()
    {
        return heartbeat;
    }

    public void setHeartbeat(int heartbeat)
    {
        this.heartbeat = heartbeat;
    }

    public int getReconnect()
    {
        return reconnect;
    }

    public void setReconnect(int reconnect)
    {
        this.reconnect = reconnect;
    }

}

