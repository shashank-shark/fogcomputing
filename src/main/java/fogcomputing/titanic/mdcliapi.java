package fogcomputing.titanic;

import java.util.Formatter;

import fogcomputing.broker.MDP;
import org.zeromq.*;

public class mdcliapi
{

    private String     broker;
    private ZContext   ctx;
    private ZMQ.Socket client;
    private long       timeout = 2500;
    private int        retries = 3;
    private boolean    verbose;
    private Formatter  log     = new Formatter(System.out);

    public long getTimeout()
    {
        return timeout;
    }

    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }

    public int getRetries()
    {
        return retries;
    }

    public void setRetries(int retries)
    {
        this.retries = retries;
    }

    public mdcliapi(String broker, boolean verbose)
    {
        this.broker = broker;
        this.verbose = verbose;
        ctx = new ZContext();
        reconnectToBroker();
    }

    void reconnectToBroker()
    {
        if (client != null) {
            ctx.destroySocket(client);
        }
        client = ctx.createSocket(ZMQ.REQ);
        client.connect(broker);
        if (verbose)
            log.format("I: connecting to broker at %s\n", broker);
    }

    public ZMsg send(String service, ZMsg request)
    {

        request.push(new ZFrame(service));
        request.push(MDP.C_CLIENT.newFrame());
        if (verbose) {
            log.format("I: send request to '%s' service: \n", service);
            request.dump(log.out());
        }
        ZMsg reply = null;

        int retriesLeft = retries;
        while (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {

            request.duplicate().send(client);

            ZMQ.Poller items = ctx.createPoller(1);
            items.register(client, ZMQ.Poller.POLLIN);
            if (items.poll(timeout) == -1)
                break; // Interrupted

            if (items.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(client);
                if (verbose) {
                    log.format("I: received reply: \n");
                    msg.dump(log.out());
                }

                assert (msg.size() >= 3);

                ZFrame header = msg.pop();
                assert (MDP.C_CLIENT.equals(header.toString()));
                header.destroy();

                ZFrame replyService = msg.pop();
                assert (service.equals(replyService.toString()));
                replyService.destroy();

                reply = msg;
                break;
            }
            else {
                items.unregister(client);
                if (--retriesLeft == 0) {
                    log.format("W: permanent error, abandoning\n");
                    break;
                }
                log.format("W: no reply, reconnecting\n");
                reconnectToBroker();
            }
            items.close();
        }
        request.destroy();
        return reply;
    }

    public void destroy()
    {
        ctx.destroy();
    }
}
