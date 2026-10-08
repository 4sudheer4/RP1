package com.rp1.ingest.engine;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BodyReaderTest {
    @Test
    void declaredLengthOverLimitRejectedWithoutReading() {
        AtomicLong reads = new AtomicLong();
        InputStream in = new InputStream() {
            @Override public int read() { reads.incrementAndGet(); return -1; }
        };
        assertThatThrownBy(() -> BodyReader.read(in, 1001, 1000)).isInstanceOf(BodyReader.TooLarge.class);
        assertThat(reads).hasValue(0);
    }

    @Test
    void undeclaredStreamStopsAtLimitWithoutBufferingTheRest() {
        AtomicLong served = new AtomicLong();
        // "infinite" chunked body: must be cut off, not read to the end (no OOM)
        InputStream endless = new InputStream() {
            @Override public int read() { served.incrementAndGet(); return 'x'; }
            @Override public int read(byte[] b, int off, int len) { served.addAndGet(len); return len; }
        };
        assertThatThrownBy(() -> BodyReader.read(endless, -1, 100_000)).isInstanceOf(BodyReader.TooLarge.class);
        assertThat(served.get()).isLessThan(100_000 + 8192 + 1);
    }

    @Test
    void exactlyAtLimitIsAccepted() throws Exception {
        byte[] data = new byte[1000];
        assertThat(BodyReader.read(new ByteArrayInputStream(data), -1, 1000)).hasSize(1000);
    }
}
