package no.ssb.dapla.parquet;

public interface FieldInterceptor {
    String intercept(String field, String value);

    static FieldInterceptor noOp() {
        return (field, value) -> value;
    }
}
