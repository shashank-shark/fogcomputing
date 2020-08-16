package fogcomputing.broker;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.zeromq.*;

public class mdbroker
{

    private static final String INTERNAL_SERVICE_PREFIX = "mmi.";
    private static final int    HEARTBEAT_LIVENESS      = 3;
    private static final int    HEARTBEAT_INTERVAL      = 2500;
    private static final int    HEARTBEAT_EXPIRY        = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;

    // ---------------------------------------------------------------------
    private static class Service
    {
        public final String name;
        Deque<ZMsg>         requests;
        Deque<Worker>       waiting;

        public Service(String name)
        {
            this.name = name;
            this.requests = new ArrayDeque<ZMsg>();
            this.waiting = new ArrayDeque<Worker>();
        }
    }

    private static class Worker
    {
        String  identity;
        ZFrame  address;
        Service service;
        long    expiry;

        public Worker(String identity, ZFrame address)
        {
            this.address = address;
            this.identity = identity;
            this.expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
        }
    }

    // ---------------------------------------------------------------------

    private ZContext   ctx;
    private ZMQ.Socket socket;

    private long                 heartbeatAt;
    private Map<String, Service> services;
    private Map<String, Worker>  workers;
    private Deque<Worker>        waiting;

    private boolean   verbose = false;
    private Formatter log  = new Formatter(System.out);

    // ---------------------------------------------------------------------

    public static void main(String[] args)
    {
        mdbroker broker = new mdbroker(args.length > 0 && "-v".equals(args[0]));
//        mdbroker broker = new mdbroker(true);
        broker.bind("tcp://*:5555");
        broker.mediate();
    }

    public mdbroker(boolean verbose)
    {
        this.verbose = verbose;
//        this.verbose = true;
        this.services = new HashMap<String, Service>();
        this.workers = new HashMap<String, Worker>();
        this.waiting = new ArrayDeque<Worker>();
        this.heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
        this.ctx = new ZContext();
        this.socket = ctx.createSocket(ZMQ.ROUTER);
    }

    public void mediate()
    {
        while (!Thread.currentThread().isInterrupted()) {
            ZMQ.Poller items = ctx.createPoller(1);
            items.register(socket, ZMQ.Poller.POLLIN);
            if (items.poll(HEARTBEAT_INTERVAL) == -1)
                break;
            if (items.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(socket);
                if (msg == null)
                    break;

                if (verbose) {
                    log.format("I: received message:\n");
                    msg.dump(log.out());
                }

                ZFrame sender = msg.pop();
                ZFrame empty = msg.pop();
                ZFrame header = msg.pop();

                if (MDP.C_CLIENT.frameEquals(header)) {
                    processClient(sender, msg);
                }
                else if (MDP.W_WORKER.frameEquals(header))
                    processWorker(sender, msg);
                else {
                    log.format("E: invalid message:\n");
                    msg.dump(log.out());
                    msg.destroy();
                }

                sender.destroy();
                empty.destroy();
                header.destroy();

            }
            items.close();
            purgeWorkers();
            sendHeartbeats();
        }
        destroy();
    }

    private void destroy()
    {
        Worker[] deleteList = workers.entrySet().toArray(new Worker[0]);
        for (Worker worker : deleteList) {
            deleteWorker(worker, true);
        }
        ctx.destroy();
    }

    private void processClient(ZFrame sender, ZMsg msg)
    {
        assert (msg.size() >= 2);
        ZFrame serviceFrame = msg.pop();
        msg.wrap(sender.duplicate());
        if (serviceFrame.toString().startsWith(INTERNAL_SERVICE_PREFIX))
            serviceInternal(serviceFrame, msg);
        else dispatch(requireService(serviceFrame), msg);
        serviceFrame.destroy();
    }

    private void processWorker(ZFrame sender, ZMsg msg)
    {
        assert (msg.size() >= 1);

        ZFrame command = msg.pop();

        boolean workerReady = workers.containsKey(sender.strhex());

        Worker worker = requireWorker(sender);

        if (MDP.W_READY.frameEquals(command)) {
            if (workerReady || sender.toString().startsWith(INTERNAL_SERVICE_PREFIX))
                deleteWorker(worker, true);
            else {
                ZFrame serviceFrame = msg.pop();
                worker.service = requireService(serviceFrame);
                workerWaiting(worker);
                serviceFrame.destroy();
            }
        }
        else if (MDP.W_REPLY.frameEquals(command)) {
            if (workerReady) {
                ZFrame client = msg.unwrap();
                msg.addFirst(worker.service.name);
                msg.addFirst(MDP.C_CLIENT.newFrame());
                msg.wrap(client);
                msg.send(socket);
                workerWaiting(worker);
            }
            else {
                deleteWorker(worker, true);
            }
        }
        else if (MDP.W_HEARTBEAT.frameEquals(command)) {
            if (workerReady) {
                worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
            }
            else {
                deleteWorker(worker, true);
            }
        }
        else if (MDP.W_DISCONNECT.frameEquals(command))
            deleteWorker(worker, false);
        else {
            log.format("E: invalid message:\n");
            msg.dump(log.out());
        }
        msg.destroy();
    }

    private void deleteWorker(Worker worker, boolean disconnect)
    {
        assert (worker != null);
        if (disconnect) {
            sendToWorker(worker, MDP.W_DISCONNECT, null, null);
        }
        if (worker.service != null)
            worker.service.waiting.remove(worker);
        workers.remove(worker);
        worker.address.destroy();
    }

    private Worker requireWorker(ZFrame address)
    {
        assert (address != null);
        String identity = address.strhex();
        Worker worker = workers.get(identity);
        if (worker == null) {
            worker = new Worker(identity, address.duplicate());
            workers.put(identity, worker);
            if (verbose)
                log.format("I: registering new worker: %s\n", identity);
        }
        return worker;
    }

    private Service requireService(ZFrame serviceFrame)
    {
        assert (serviceFrame != null);
        String name = serviceFrame.toString();
        Service service = services.get(name);
        if (service == null) {
            service = new Service(name);
            services.put(name, service);
        }
        return service;
    }

    private void bind(String endpoint)
    {
        socket.bind(endpoint);
        log.format("I: MDP broker/0.1.1 is active at %s\n", endpoint);
    }

    private void serviceInternal(ZFrame serviceFrame, ZMsg msg)
    {
        String returnCode = "501";
        if ("mmi.service".equals(serviceFrame.toString())) {
            String name = msg.peekLast().toString();
            returnCode = services.containsKey(name) ? "200" : "400";
        }
        msg.peekLast().reset(returnCode.getBytes(ZMQ.CHARSET));
        ZFrame client = msg.unwrap();
        msg.addFirst(serviceFrame.duplicate());
        msg.addFirst(MDP.C_CLIENT.newFrame());
        msg.wrap(client);
        msg.send(socket);
    }

    public synchronized void sendHeartbeats()
    {
        if (System.currentTimeMillis() >= heartbeatAt) {
            for (Worker worker : waiting) {
                sendToWorker(worker, MDP.W_HEARTBEAT, null, null);
            }
            heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
        }
    }

    public synchronized void purgeWorkers()
    {
        for (Worker w = waiting.peekFirst(); w != null
                && w.expiry < System.currentTimeMillis(); w = waiting.peekFirst()) {
            log.format("I: deleting expired worker: %s\n", w.identity);
            deleteWorker(waiting.pollFirst(), false);
        }
    }

    public synchronized void workerWaiting(Worker worker)
    {
        // Queue to broker and service waiting lists
        waiting.addLast(worker);
        worker.service.waiting.addLast(worker);
        worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
        dispatch(worker.service, null);
    }

    private void dispatch(Service service, ZMsg msg)
    {
        assert (service != null);
        if (msg != null)
            service.requests.offerLast(msg);
        purgeWorkers();
        while (!service.waiting.isEmpty() && !service.requests.isEmpty()) {
            msg = service.requests.pop();
            Worker worker = service.waiting.pop();
            waiting.remove(worker);
            sendToWorker(worker, MDP.W_REQUEST, null, msg);
            msg.destroy();
        }
    }

    public void sendToWorker(Worker worker, MDP command, String option, ZMsg msgp)
    {

        ZMsg msg = msgp == null ? new ZMsg() : msgp.duplicate();

        if (option != null)
            msg.addFirst(new ZFrame(option));
        msg.addFirst(command.newFrame());
        msg.addFirst(MDP.W_WORKER.newFrame());

        msg.wrap(worker.address.duplicate());
        if (verbose) {
            log.format("I: sending %s to worker\n", command);
            msg.dump(log.out());
        }
        msg.send(socket);
    }
}