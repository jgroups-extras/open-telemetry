package org.jgroups.open_telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.List;
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

        InMemorySpanExporter spanExporter=InMemorySpanExporter.create();

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

        // otel=GlobalOpenTelemetry.get();

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

        for(int i=1; i <= 5; i++)
            send(i);
        Util.waitUntil(10000, 500, () -> spans.isEmpty());
        Util.close(b,a);
        List<SpanData> span_items=spanExporter.getFinishedSpanItems();
        System.out.printf("-- spans (%d):\n%s\n", span_items.size(), span_items);
    }

    // sends a unicast message from A -> B
    protected void send(int num) throws Exception {
        Span span = tracer.spanBuilder("send").setSpanKind(SpanKind.CLIENT).startSpan();
        spans.put(num, span);

        try (Scope ignored = span.makeCurrent()) {
            span.addEvent("sending " + num);
            span.setAttribute(AttributeKey.stringKey("destination"), b_addr.toString());
            TracerHeader hdr=new TracerHeader();
            populateHeader(hdr);
            Message msg=new ObjectMessage(b_addr, num)
              .putHeader(HDR_ID, hdr);
            a.send(msg);
        } catch (Throwable throwable) {
            span.setStatus(StatusCode.ERROR, "Error during execution of my span operation!");
            span.recordException(throwable);
            span.end();
            throw throwable;
        }
    }

    public static void populateHeader(TracerHeader hdr) {
        // Inject the request with the *current* Context, which contains our current Span.
        W3CTraceContextPropagator.getInstance()
          .inject(Context.current(), hdr,
                  (carrier, key, value) -> hdr.put(key, value));
    }




    public static void main( String[] args ) throws Exception {
        new JGroupsApp().start();
    }

    // the receiver
    protected class RequestHandler implements Receiver {
        protected final JChannel ch;

        protected RequestHandler(JChannel ch) {
            this.ch=ch;
        }

        public void receive(Message msg) {
            TracerHeader hdr=msg.getHeader(HDR_ID);
            int num=msg.getObject();
            System.out.printf("-- received msg %d, ctx: %s\n", num, hdr);


            Context extractedContext = otel.getPropagators().getTextMapPropagator()
              .extract(Context.current(), hdr, REST_REQUEST_TEXT_MAP_GETTER);

            Span span = tracer.spanBuilder("receive")
              .setSpanKind(SpanKind.SERVER)
              .setParent(extractedContext).startSpan();

            try (Scope ignored = span.makeCurrent()) {
                span.addEvent("receiving " + num);
                Util.sleepRandom(100, 500); // 'processing time'

                Message rsp=new ObjectMessage(msg.src(), num);
                ch.send(rsp);
            } catch (Throwable throwable) {
                span.setStatus(StatusCode.ERROR, "Error during execution of my span operation!");
                span.recordException(throwable);
            }
            finally {
                span.end();
            }
        }
    }

    private static final TextMapGetter<TracerHeader> REST_REQUEST_TEXT_MAP_GETTER =
      new TextMapGetter<>() {
          @Override
          public String get(TracerHeader carrier, String key) {
              return carrier.get(key);
          }

          @Override
          public Iterable<String> keys(TracerHeader carrier) {
              return carrier.keys();
          }
      };

    // the sender
    protected class ResponseHandler implements Receiver {
        public void receive(Message msg) {
            int num=msg.getObject();
            System.out.printf("-- received response to request %d\n", num);
            Span span=spans.remove(num);
            span.addEvent("end");
            span.setStatus(StatusCode.OK);
            span.end();
        }
    }

}
