package org.jgroups.open_telemetry;

import org.jgroups.Header;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Header which carries an OpenTelemetry {@link io.opentelemetry.api.trace.Span} between requests and responses
 * @author Bela Ban
 * @since  1.0.0
 */
public class TracerHeader extends Header {
    protected static final short ID=1050;
    protected Map<String,String> ctx=new HashMap<>();

    static {
        ClassConfigurator.add(ID, TracerHeader.class);
    }

    public TracerHeader() {
    }

    public TracerHeader(Map<String,String> ctx) {
        this.ctx=ctx;
    }

    public Map<String,String>  getSpanContext()                     {return ctx;}
    public TracerHeader        setSpanContext(Map<String,String> s) {this.ctx=s; return this;}

    public short getMagicId() {
        return ID;
    }

    public Supplier<? extends Header> create() {
        return TracerHeader::new;
    }

    public void put(String key, String value) {
        ctx.put(key, value);
    }

    public String get(String key) {return ctx.get(key);}

    public Set<String> keys() {return ctx.keySet();}

    public int serializedSize() {
        int size=0;
        int num_attrs=ctx.size();
        size+=num_attrs;
        if(num_attrs > 0) {
            for(Map.Entry<String,String> entry: ctx.entrySet()) {
                String key=entry.getKey();
                String val=entry.getValue();
                size+=Util.size(key) + Util.size(val);
            }
        }
        return size;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(ctx.size());
        if(!ctx.isEmpty()) {
            for(Map.Entry<String,String> e: ctx.entrySet()) {
                Util.writeString(e.getKey(), out);
                Util.writeString(e.getValue(), out);
            }
        }
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size=in.readInt();
        if(size > 0) {
            for(int i=0; i < size; i++)
                ctx.put(Util.readString(in), Util.readString(in));
        }
    }
}
