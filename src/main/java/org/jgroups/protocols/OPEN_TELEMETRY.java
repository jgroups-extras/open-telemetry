package org.jgroups.protocols;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.open_telemetry.TracerHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides Open Telemetry (https://opentelemetry.io/) tracing for JGroups. It should be placed just above the
 * transport.<br/>
 * When a message is sent, a {@link org.jgroups.open_telemetry.TracerHeader} is added with the (optional) parent span.
 * When received a new span is started (as a child span, if the parent span in the header is non-null), and ended when
 * the the thread returns.
 * @author Bela Ban
 * @since  1.0.0
 */
@MBean(description="Records OpenTelemetry traces of sent and received messages")
public class OPEN_TELEMETRY extends Protocol {
    public static final short OPEN_TELEMETRY_ID=550; // as defined in jg-protocols.xml
    protected OpenTelemetry   otel;
    protected Tracer          tracer;

    @Property(description="When active, traces are recorded, otherwise not")
    protected boolean         active=true;

    static {
        ClassConfigurator.addProtocol(OPEN_TELEMETRY_ID, OPEN_TELEMETRY.class);
    }

    public boolean        active()          {return active;}

    public OPEN_TELEMETRY active(boolean f) {active=activate(f); return this;}

    public void start() throws Exception {
        super.start();
        activate(active);
    }

    public Object down(Message msg) {
        if(!active)
            return down_prot.down(msg);
        TracerHeader hdr=new TracerHeader();
        populateHeader(hdr); // will populate if a span exists (created by the caller)
        msg.putHeader(OPEN_TELEMETRY_ID, hdr);
        return down_prot.down(msg);
    }


    public Object up(Message msg) {
        if(!active)
            return up_prot.up(msg);

        TracerHeader hdr=msg.getHeader(OPEN_TELEMETRY_ID);
        Context extractedContext=otel.getPropagators().getTextMapPropagator()
          .extract(Context.current(), hdr, TEXT_MAP_GETTER);

        Span span=tracer.spanBuilder("deliver-single-msg")
          .setSpanKind(SpanKind.SERVER)
          .setParent(extractedContext).startSpan();

        try(Scope ignored=span.makeCurrent()) {
            span.setAttribute("from", msg.src().toString());
            return up_prot.up(msg);
        }
        catch(Throwable t) {
            span.setStatus(StatusCode.ERROR, String.format("failed delivering single message from %s", msg.src()));
            span.recordException(t);
            throw t;
        }
        finally {
            span.end();
        }
    }

    public void up(MessageBatch batch) {
        if(!active) {
            if(!batch.isEmpty())
                up_prot.up(batch);
            return;
        }
        List<Span> spans=new ArrayList<>(batch.size());
        int index=0, batch_size=batch.size();
        for(Message msg: batch) {
            index++;
            TracerHeader hdr=msg.getHeader(OPEN_TELEMETRY_ID);
            Context extractedContext=otel.getPropagators().getTextMapPropagator()
              .extract(Context.current(), hdr, TEXT_MAP_GETTER);

            Span span=tracer.spanBuilder("deliver-batched-msg")
              .setSpanKind(SpanKind.SERVER)
              .setParent(extractedContext).startSpan();
            span.setAttribute("batch-msg", String.format("%d/%d", index, batch_size));
            spans.add(span);
        }
        try {
            if(!batch.isEmpty())
                up_prot.up(batch);
        }
        catch(Throwable t) {
            spans.forEach(s -> {
                s.setStatus(StatusCode.ERROR, String.format("failed delivering batched message from %s", batch.sender()))
                  .recordException(t);
            });
            throw t;
        }
        finally {
            spans.forEach(Span::end);
        }
    }

    protected static void populateHeader(TracerHeader hdr) {
        // Inject the request with the *current* Context, which contains our current Span.
        W3CTraceContextPropagator.getInstance().inject(Context.current(), hdr,(carrier, key, val) -> hdr.put(key, val));
    }

    protected static final TextMapGetter<TracerHeader> TEXT_MAP_GETTER =
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

    protected boolean activate(boolean flag) {
        if(flag && otel == null)
            otel=GlobalOpenTelemetry.get();
        if(flag && tracer == null)
            tracer=otel.getTracer("org.jgroups.trace", Version.printVersion());
        return flag;
    }

}
