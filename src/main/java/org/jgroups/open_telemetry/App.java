package org.jgroups.open_telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.jgroups.Version;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class App {
    protected OpenTelemetry otel;
    protected Tracer        tracer;
    DataOutput output=new ByteArrayDataOutputStream();


    TextMapSetter<DataOutput> setter =
      (carrier, key, value) -> {
          try {
              System.out.printf("%s -> %s\n", key, value);
              carrier.writeBytes(key);
              carrier.writeBytes(value);
          }
          catch(Throwable t) {
              System.err.printf("exception: %s", t);
          }
      };

    protected App(OpenTelemetry otel) {
        this.otel=otel;
        tracer=otel.getTracer("org.jgroups.trace", Version.printVersion());
    }

    protected void start() throws Exception {

        Span span = tracer.spanBuilder("parent-span").setSpanKind(SpanKind.CLIENT).startSpan();


        try (Scope scope = span.makeCurrent()) {
            otel.getPropagators().getTextMapPropagator().inject(Context.current(), output, setter);

            Thread.sleep(30);
            span.addEvent("init");
            executeChild(tracer);
            span.addEvent("end");
            Thread.sleep(30);
            span.setStatus(StatusCode.OK);

            SpanContext ctx=span.getSpanContext();
            System.out.println("ctx = " + ctx);

        } catch (Throwable throwable) {
            span.setStatus(StatusCode.ERROR, "Error during execution of my span operation!");
            span.recordException(throwable);
            throw throwable;
        } finally {

            span.end();
        }





        System.out.println("span = " + span);


    }

    protected void executeChild(Tracer t) throws InterruptedException {
        Random random = new Random();

        Span childSpan1 = t.spanBuilder("child-1").startSpan();
        try(Scope scope = childSpan1.makeCurrent()) {
            childSpan1.setAttribute("foo", random.nextInt());
            Thread.sleep(500);
            childSpan1.setStatus(StatusCode.OK);

            Map<String,String> map=getContextMap();
            System.out.println("map = " + map);

            // otel.getPropagators().getTextMapPropagator().inject(Context.current(), output, setter);
        } finally {
            childSpan1.end();
        }

        Span childSpan2 = t.spanBuilder("child-2").startSpan();
        try(Scope scope = childSpan2.makeCurrent()) {
            childSpan2.setAttribute("bar", random.nextInt());
            Thread.sleep(500);
            childSpan2.setStatus(StatusCode.OK);
        } finally {
            childSpan2.end();
        }
    }


    public static Map<String, String> getContextMap() {
        HashMap<String, String> result = new HashMap<>();

        // Inject the request with the *current* Context, which contains our current Span.
        W3CTraceContextPropagator.getInstance().inject(Context.current(), result,
                                                       (carrier, key, value) -> carrier.put(key, value));
        return result;
    }


    public static void main( String[] args ) throws Exception {
        SpanExporter spanExporter=InMemorySpanExporter.create();

        /*SpanExporter spanExporter=JaegerGrpcSpanExporter.builder()
                       .setEndpoint("http://localhost:14250")
                       .build();*/

        Resource resource=null;

        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);
        SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
          .addSpanProcessor(spanProcessor);

        if (resource != null) {
            builder.setResource(Resource.getDefault().merge(resource));
        }

        SdkTracerProvider tracerProvider=builder.build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
          .setTracerProvider(tracerProvider)
          .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
          .buildAndRegisterGlobal();

        /*openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
                    // At the moment we don't export Infinispan server metrics with OpenTelemetry,
                    // so we manually disable any metrics export
                    .addPropertiesSupplier(() -> Collections.singletonMap("otel.metrics.exporter", "none"))
                    .build()
                    .getOpenTelemetrySdk();*/


        App app=new App(openTelemetry);
        app.start();
    }


}
