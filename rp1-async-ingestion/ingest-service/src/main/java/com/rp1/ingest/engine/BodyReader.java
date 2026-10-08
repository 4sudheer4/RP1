package com.rp1.ingest.engine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads the request body with a hard cap. The declared Content-Length is checked first (cheap rejection); the
 * stream is then read in chunks and abandoned as soon as it exceeds the cap, so chunked or lying clients cannot
 * make us buffer more than {@code max + chunk} bytes.
 */
public final class BodyReader {
    public static final class TooLarge extends Exception {
        TooLarge() { super(null, null, false, false); }
    }

    private BodyReader() {}

    public static byte[] read(InputStream in, long declaredLength, int max) throws IOException, TooLarge {
        if (declaredLength > max) throw new TooLarge();
        ByteArrayOutputStream out = new ByteArrayOutputStream(declaredLength > 0 ? (int) declaredLength : 8192);
        byte[] buf = new byte[8192];
        int total = 0;
        int n;
        while ((n = in.read(buf)) != -1) {
            total += n;
            if (total > max) throw new TooLarge();
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }
}
