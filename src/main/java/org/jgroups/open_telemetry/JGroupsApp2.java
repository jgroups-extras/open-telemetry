package org.jgroups.open_telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.jgroups.*;
import org.jgroups.protocols.OPEN_TELEMETRY;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Demos sending a span across nodes, using {@link org.jgroups.protocols.OPEN_TELEMETRY} in the stack
 */
public class JGroupsApp2 {
    protected JChannel                   a,b;
    protected OpenTelemetry              otel;
    protected Tracer tracer;
    protected static final int           NUM_REQS=5;
    protected CompletableFuture<Integer> cf;

    protected void start() throws Exception {
        // InMemorySpanExporter spanExporter=InMemorySpanExporter.create();

        JaegerGrpcSpanExporter spanExporter=JaegerGrpcSpanExporter.builder()
          .setEndpoint("http://localhost:14250")
          .build();

        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
          .addSpanProcessor(spanProcessor);

        SdkTracerProvider tracerProvider=builder.build();
        otel = OpenTelemetrySdk.builder()
          .setTracerProvider(tracerProvider)
          .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
          .buildAndRegisterGlobal();
        tracer=otel.getTracer("application", "1.0.0");

        ResponseHandler rsp_handler=new ResponseHandler();
        a=new JChannel(Util.getTestStack()).name("A").setReceiver(rsp_handler);
        b=new JChannel(Util.getTestStack()).name("B").setReceiver(new RequestHandler(b));

        for(JChannel ch: Arrays.asList(a,b))
            ch.getProtocolStack().insertProtocol(new OPEN_TELEMETRY(), ProtocolStack.Position.ABOVE, TP.class);

        a.connect(JGroupsApp2.class.getSimpleName());
        b.connect(JGroupsApp2.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b);
        Address b_addr=Objects.requireNonNull(b.getAddress());
        b.setReceiver(new RequestHandler(b));

        // Start application span

        for(int i=1; i <= NUM_REQS; i++) {
            send(b_addr, i);
        }
        Util.close(a,b);

        Util.sleep(1000); // give jaeger time to send the data to the server

        // List<SpanData> span_items=spanExporter.getFinishedSpanItems();
        // System.out.printf("-- spans (%d):\n%s\n", span_items.size(), span_items);
    }

    // sends a unicast message from A -> B
    protected void send(Address dest, int num) throws Exception {
        Span span=this.tracer.spanBuilder("app-send").setSpanKind(SpanKind.CLIENT).startSpan();
        try(Scope ignored=span.makeCurrent()) {
            cf=new CompletableFuture<>();
            Message msg=new ObjectMessage(dest, num);
            long start=System.currentTimeMillis();
            a.send(msg);
            Integer result=cf.get(5, TimeUnit.SECONDS);
            long time=System.currentTimeMillis() - start;
            System.out.printf("-- received rsp %d after %d ms\n", result, time);
        }
        catch(Throwable t) {
            span.setStatus(StatusCode.ERROR, "failed sending app messages").recordException(t);
        }
        finally {
            span.end();
        }
    }

    public static void populateHeader(TracerHeader hdr) {
        // Inject the request with the *current* Context, which contains our current Span.
        W3CTraceContextPropagator.getInstance()
          .inject(Context.current(), hdr,
                  (carrier, key, value) -> hdr.put(key, value));
    }




    public static void main( String[] args ) throws Exception {
        new JGroupsApp2().start();
    }

    // the receiver
    protected static class RequestHandler implements Receiver {
        protected final JChannel ch;

        protected RequestHandler(JChannel ch) {
            this.ch=ch;
        }

        public void receive(Message msg) {
            TracerHeader hdr=msg.getHeader(OPEN_TELEMETRY.OPEN_TELEMETRY_ID);
            int num=msg.getObject();
            System.out.printf("-- received request %d, ctx: %s\n", num, hdr);
            Message rsp=new ObjectMessage(msg.src(), num);
            try {
                ch.send(rsp);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    // the sender
    protected class ResponseHandler implements Receiver {
        public void receive(Message msg) {
            int num=msg.getObject();
            cf.complete(num);
        }
    }

}
