package com.rp1.ingest.engine;

import com.rp1.ingest.config.IngestProperties.Signature.KeyEncoding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SignatureVerifierTest {
    // Shaped like an SFMC signatureKey (base64-looking string); used as UTF-8 bytes by default.
    static final String CURRENT = "V27FXfqI3DnhfQW1bhFDeJixpt8eDAY5R24UJI3cK6M=";
    static final String PREVIOUS = "KktBXb9V556KtSDG7Wb5a0xHAu/z9n3k+YekDzLy7i0=";
    final byte[] body = "[{\"eventCategoryType\":\"TransactionalSendEvents.EmailSent\"}]".getBytes(StandardCharsets.UTF_8);

    private static String sign(byte[] body, String key) {
        return SignatureVerifier.sign(body, key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void currentAndPreviousKeysAccepted() {
        var v = new SignatureVerifier(List.of(CURRENT, PREVIOUS), KeyEncoding.UTF8);
        assertThat(v.verify(body, sign(body, CURRENT))).isZero();
        assertThat(v.verify(body, sign(body, PREVIOUS))).isEqualTo(1);
    }

    @Test
    void wrongKeyTamperedBodyAndGarbageRejected() {
        var v = new SignatureVerifier(List.of(CURRENT), KeyEncoding.UTF8);
        assertThat(v.verify(body, sign(body, PREVIOUS))).isEqualTo(SignatureVerifier.NO_MATCH);
        byte[] tampered = body.clone();
        tampered[5] ^= 1;
        assertThat(v.verify(tampered, sign(body, CURRENT))).isEqualTo(SignatureVerifier.NO_MATCH);
        assertThat(v.verify(body, "not base64!!")).isEqualTo(SignatureVerifier.NO_MATCH);
        assertThat(v.verify(body, Base64.getEncoder().encodeToString(new byte[16]))).isEqualTo(SignatureVerifier.NO_MATCH);
    }

    @Test
    void base64KeyEncodingDecodesKey() {
        var v = new SignatureVerifier(List.of(CURRENT), KeyEncoding.BASE64);
        String sig = SignatureVerifier.sign(body, Base64.getDecoder().decode(CURRENT));
        assertThat(v.verify(body, sig)).isZero();
        // and the two encodings are genuinely different keys
        assertThat(v.verify(body, sign(body, CURRENT))).isEqualTo(SignatureVerifier.NO_MATCH);
    }

    @Test
    void noKeysIsAStartupError() {
        assertThatThrownBy(() -> new SignatureVerifier(List.of(), KeyEncoding.UTF8)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignatureVerifier(List.of(""), KeyEncoding.UTF8)).isInstanceOf(IllegalArgumentException.class);
    }
}
