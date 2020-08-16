package fogcomputing.titanic;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;
import org.zeromq.ZThread.IAttachedRunnable;
import org.zeromq.ZThread.IDetachedRunnable;

public class titanic
{
    static String generateUUID()
    {
        return UUID.randomUUID().toString();
    }

    private static final String TITANIC_DIR = ".titanic";

    private static String requestFilename(String uuid)
    {
        String filename = String.format("%s/%s.req", TITANIC_DIR, uuid);
        return filename;
    }

    private static String replyFilename(String uuid)
    {
        String filename = String.format("%s/%s.rep", TITANIC_DIR, uuid);
        return filename;
    }

    static class TitanicRequest implements IAttachedRunnable
    {
        @Override
        public void run(Object[] args, ZContext ctx, Socket pipe)
        {
            mdwrkapi worker = new mdwrkapi(
                    "tcp://localhost:5555", "titanic.request", true
            );
            ZMsg reply = null;

            while (true) {

                ZMsg request = worker.receive(reply);
                if (request == null)
                    break;


                new File(TITANIC_DIR).mkdirs();

                String uuid = generateUUID();
                String filename = requestFilename(uuid);
                DataOutputStream file = null;
                try {
                    file = new DataOutputStream(new FileOutputStream(filename));
                    ZMsg.save(request, file);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    try {
                        if (file != null)
                            file.close();
                    }
                    catch (IOException e) {
                    }
                }
                request.destroy();


                reply = new ZMsg();
                reply.add(uuid);
                reply.send(pipe);

                reply = new ZMsg();
                reply.add("200");
                reply.add(uuid);
            }
            worker.destroy();
        }
    }

    static class TitanicReply implements IDetachedRunnable
    {
        @Override
        public void run(Object[] args)
        {
            mdwrkapi worker = new mdwrkapi(
                    "tcp://localhost:5555", "titanic.reply", true
            );
            ZMsg reply = null;

            while (true) {
                ZMsg request = worker.receive(reply);
                if (request == null)
                    break;

                String uuid = request.popString();
                String reqFilename = requestFilename(uuid);
                String repFilename = replyFilename(uuid);

                if (new File(repFilename).exists()) {
                    DataInputStream file = null;
                    try {
                        file = new DataInputStream(
                                new FileInputStream(repFilename)
                        );
                        reply = ZMsg.load(file);
                        reply.push("200");
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    finally {
                        try {
                            if (file != null)
                                file.close();
                        }
                        catch (IOException e) {
                        }
                    }
                }
                else {
                    reply = new ZMsg();
                    if (new File(reqFilename).exists())
                        reply.push("300");
                    else reply.push("400");
                }
                request.destroy();
            }
            worker.destroy();
        }
    }

    static class TitanicClose implements IDetachedRunnable
    {
        @Override
        public void run(Object[] args)
        {
            mdwrkapi worker = new mdwrkapi(
                    "tcp://localhost:5555", "titanic.close", true
            );
            ZMsg reply = null;

            while (true) {
                ZMsg request = worker.receive(reply);
                if (request == null)
                    break;

                String uuid = request.popString();
                String req_filename = requestFilename(uuid);
                String rep_filename = replyFilename(uuid);
                new File(rep_filename).delete();
                new File(req_filename).delete();

                request.destroy();
                reply = new ZMsg();
                reply.add("200");
            }
            worker.destroy();
        }
    }

    public static void main(String[] args)
    {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));

        try (ZContext ctx = new ZContext()) {
            Socket requestPipe = ZThread.fork(ctx, new TitanicRequest());
            ZThread.start(new TitanicReply());
            ZThread.start(new TitanicClose());

            Poller poller = ctx.createPoller(1);
            poller.register(requestPipe, ZMQ.Poller.POLLIN);


            while (true) {

                int rc = poller.poll(1000);
                if (rc == -1)
                    break;
                if (poller.pollin(0)) {

                    new File(TITANIC_DIR).mkdirs();

                    ZMsg msg = ZMsg.recvMsg(requestPipe);
                    if (msg == null)
                        break; //  Interrupted
                    String uuid = msg.popString();
                    BufferedWriter wfile = null;
                    try {
                        wfile = new BufferedWriter(
                                new FileWriter(TITANIC_DIR + "/queue", true)
                        );
                        wfile.write("-" + uuid + "\n");
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        break;
                    }
                    finally {
                        try {
                            if (wfile != null)
                                wfile.close();
                        }
                        catch (IOException e) {
                        }
                    }
                    msg.destroy();
                }

                byte[] entry = new byte[37];

                RandomAccessFile file = null;

                try {
                    file = new RandomAccessFile(TITANIC_DIR + "/queue", "rw");
                    while (file.read(entry) > 0) {
                        if (entry[0] == '-') {
                            if (verbose)
                                System.out.printf(
                                        "I: processing request %s\n",
                                        new String(
                                                entry, 1, entry.length - 1, ZMQ.CHARSET
                                        )
                                );
                            if (serviceSuccess(
                                    new String(
                                            entry, 1, entry.length - 1, ZMQ.CHARSET
                                    )
                            )) {
                                file.seek(file.getFilePointer() - 37);
                                file.writeBytes("+");
                                file.seek(file.getFilePointer() + 36);
                            }
                        }

                        if (file.readByte() == '\r')
                            file.readByte();

                        if (Thread.currentThread().isInterrupted())
                            break;
                    }
                }
                catch (FileNotFoundException e) {
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    if (file != null) {
                        try {
                            file.close();
                        }
                        catch (IOException e) {
                        }
                    }
                }
            }
        }
    }

    static boolean serviceSuccess(String uuid)
    {

        String filename = requestFilename(uuid);

        if (!new File(filename).exists())
            return true;

        DataInputStream file = null;
        ZMsg request;
        try {
            file = new DataInputStream(new FileInputStream(filename));
            request = ZMsg.load(file);
        }
        catch (IOException e) {
            e.printStackTrace();
            return true;
        }
        finally {
            try {
                if (file != null)
                    file.close();
            }
            catch (IOException e) {
            }
        }
        ZFrame service = request.pop();
        String serviceName = service.toString();

        mdcliapi client = new mdcliapi("tcp://localhost:5555", false);
        client.setTimeout(1000);
        client.setRetries(1);

        ZMsg mmiRequest = new ZMsg();
        mmiRequest.add(service);
        ZMsg mmiReply = client.send("mmi.service", mmiRequest);
        boolean serviceOK = (mmiReply != null &&
                mmiReply.getFirst().toString().equals("200"));
        mmiReply.destroy();

        boolean result = false;
        if (serviceOK) {
            ZMsg reply = client.send(serviceName, request);
            if (reply != null) {
                filename = replyFilename(uuid);
                DataOutputStream ofile = null;
                try {
                    ofile = new DataOutputStream(new FileOutputStream(filename));
                    ZMsg.save(reply, ofile);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return true;
                }
                finally {
                    try {
                        if (file != null)
                            file.close();
                    }
                    catch (IOException e) {
                    }
                }
                result = true;
            }
            reply.destroy();
        }
        else request.destroy();

        client.destroy();
        return result;
    }
}

