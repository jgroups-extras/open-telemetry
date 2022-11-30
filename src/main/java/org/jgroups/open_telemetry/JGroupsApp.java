package org.jgroups.open_telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Demos sending a span across nodes
 */
public class JGroupsApp {
    protected JChannel           a,b;
    protected Address            b_addr;
    protected static final short HDR_ID=1965;
    protected OpenTelemetry      otel;
    protected Tracer             tracer;
    protected Map<Integer,Span>  spans=new ConcurrentHashMap<>(); // needed?


    protected void start() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A")
          .setReceiver(new ResponseHandler())
          .connect("JGroupsApp");
        b=new JChannel(Util.getTestStack()).name("B")
          .connect("JGroupsApp");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b);
        b_addr=Objects.requireNonNull(b.getAddress());
        b.setReceiver(new RequestHandler(b));

        SpanExporter spanExporter=InMemorySpanExporter.create();

        /*SpanExporter spanExporter=JaegerGrpcSpanExporter.builder()
                              .setEndpoint("http://localhost:14250")
                              .build();*/

        Resource resource=null;

        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
          .addSpanProcessor(spanProcessor);

        if (resource != null)
            builder.setResource(Resource.getDefault().merge(resource));

        SdkTracerProvider tracerProvider=builder.build();

        otel = OpenTelemetrySdk.builder()
          .setTracerProvider(tracerProvider)
          .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
          .buildAndRegisterGlobal();

        /*openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
                           // At the moment we don't export Infinispan server metrics with OpenTelemetry,
                           // so we manually disable any metrics export
                           .addPropertiesSupplier(() -> Collections.singletonMap("otel.metrics.exporter", "none"))
                           .build()
                           .getOpenTelemetrySdk();*/

        tracer=otel.getTracer("org.jgroups.trace", Version.printVersion());

        for(int i=1; i <= 10; i++)
            send(i);
        Util.waitUntil(5000, 500, () -> spans.isEmpty());
        Util.close(b,a);
    }

    // sends a unicast message from A -> B
    protected void send(int num) throws Exception {
        Span span = tracer.spanBuilder("requester").setSpanKind(SpanKind.CLIENT).startSpan();
        spans.put(num, span);

        try (Scope ignored = span.makeCurrent()) {
            span.addEvent("sending");
            span.addEvent("end");
            Thread.sleep(30);


            SpanContext ctx=span.getSpanContext();

            Message msg=new ObjectMessage(b_addr, num)
              .putHeader(HDR_ID, new TracerHeader(ctx));
            a.send(msg);
        } catch (Throwable throwable) {
            span.setStatus(StatusCode.ERROR, "Error during execution of my span operation!");
            span.recordException(throwable);
            throw throwable;
        } finally {
            span.end();
        }
    }





    public static void main( String[] args ) throws Exception {
        new JGroupsApp().start();
    }

    // the receiver
    protected static class RequestHandler implements Receiver {
        protected final JChannel ch;

        protected RequestHandler(JChannel ch) {
            this.ch=ch;
        }

        public void receive(Message msg) {
            TracerHeader hdr=msg.getHeader(HDR_ID);
            int num=msg.getObject();
            System.out.printf("-- received msg %d, ctx: %s\n", num, hdr.getSpanContext());

            Util.sleepRandom(100, 500); // 'processing time'
            Message rsp=new ObjectMessage(msg.src(), num);
            try {
                ch.send(rsp);
            }
            catch(Exception ex) {
                System.err.printf("sending of response failed: %s", ex);
            }
        }
    }

    // the sender
    protected class ResponseHandler implements Receiver {
        public void receive(Message msg) {
            int num=msg.getObject();
            System.out.printf("-- received response to request %d\n", num);
            Span span=spans.remove(num);
            span.setStatus(StatusCode.OK);
        }
    }

}
