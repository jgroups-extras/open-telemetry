package org.jgroups.open_telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

/**
 * Hello world!
 *
 */
public class App {
    protected static Tracer tracer;

    public static void main( String[] args ) throws InterruptedException {
        System.out.println( "Hello World!" );


        tracer=OpenTelemetry.noop().getTracer("jgroups");
        parentOne();
    }


    static void parentOne() {
      Span parentSpan = tracer.spanBuilder("parent").startSpan();
      try {
        childOne(parentSpan);
      } finally {
          System.out.println("parentSpan = " + parentSpan);
        parentSpan.end();
      }
    }

    static void childOne(Span parentSpan) {
      Span childSpan = tracer.spanBuilder("child")
            .setParent(Context.current().with(parentSpan))
            .startSpan();
      try {
        Thread.sleep(500);
      }
      catch(InterruptedException e) {
          throw new RuntimeException(e);
      }
      finally {
        childSpan.end();
      }
    }


}
