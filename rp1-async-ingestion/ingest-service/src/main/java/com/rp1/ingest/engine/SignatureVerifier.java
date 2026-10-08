package com.rp1.ingest.engine;

import com.rp1.ingest.config.IngestProperties.Signature.KeyEncoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;

/**
 * SFMC ENS signature: header {@code x-sfmc-ens-signature} = base64(HMAC-SHA256(signatureKey, raw body)).
 * The HMAC is over the exact received bytes (never a re-serialization). Comparison is constant time
 * ({@link MessageDigest#isEqual}). Multiple keys support rotation: index 0 = current, 1+ = previous.
 */
public final class SignatureVerifier {
    public static final String HEADER = "x-sfmc-ens-signature";
    /** No key matched (or the header was not valid base64 of a 32-byte MAC). */
    public static final int NO_MATCH = -1;

    private final List<SecretKeySpec> keys;

    public SignatureVerifier(List<String> keys, KeyEncoding encoding) {
        if (keys == null || keys.isEmpty() || keys.stream().anyMatch(k -> k == null || k.isBlank())) {
            throw new IllegalArgumentException("signature verification enabled but no keys configured (RP1_SIGNATURE_KEYS)");
        }
        this.keys = keys.stream().map(k -> new SecretKeySpec(keyBytes(k, encoding), "HmacSHA256")).toList();
    }

    /** @return index of the matching key (0 = current), or {@link #NO_MATCH} */
    public int verify(byte[] body, String headerValue) {
        byte[] presented;
        try {
            presented = Base64.getDecoder().decode(headerValue.trim());
        } catch (IllegalArgumentException e) {
            return NO_MATCH;
        }
        if (presented.length != 32) return NO_MATCH;
        int match = NO_MATCH;
        // Check every key even after a match so timing does not reveal which key matched.
        for (int i = 0; i < keys.size(); i++) {
            if (MessageDigest.isEqual(hmac(keys.get(i), body), presented) && match == NO_MATCH) match = i;
        }
        return match;
    }

    public static String sign(byte[] body, byte[] key) {
        return Base64.getEncoder().encodeToString(hmac(new SecretKeySpec(key, "HmacSHA256"), body));
    }

    private static byte[] hmac(SecretKeySpec key, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256"); // Mac is not thread-safe; instances are cheap
            mac.init(key);
            return mac.doFinal(body);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] keyBytes(String key, KeyEncoding encoding) {
        return encoding == KeyEncoding.BASE64 ? Base64.getDecoder().decode(key) : key.getBytes(StandardCharsets.UTF_8);
    }
}
