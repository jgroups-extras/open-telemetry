package org.jgroups.open_telemetry;

import io.opentelemetry.api.trace.SpanContext;
import org.jgroups.Header;
import org.jgroups.conf.ClassConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Header which carries an OpenTelemetry {@link io.opentelemetry.api.trace.Span} between requests and responses
 * @author Bela Ban
 * @since  1.0.0
 */
public class TracerHeader extends Header {
    protected static final short ID=1050;
    protected SpanContext        span;

    static {
        ClassConfigurator.add(ID, TracerHeader.class);
    }

    public TracerHeader() {
    }

    public TracerHeader(SpanContext span) {
        this.span=span;
    }

    public SpanContext  getSpanContext()              {return span;}
    public TracerHeader setSpanContext(SpanContext s) {this.span=s; return this;}

    public short getMagicId() {
        return ID;
    }

    public Supplier<? extends Header> create() {
        return TracerHeader::new;
    }

    public int serializedSize() {
        // todo: get the number of bytes in the serialized state
        return 0;
    }

    public void writeTo(DataOutput out) throws IOException {

    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {

    }
}
