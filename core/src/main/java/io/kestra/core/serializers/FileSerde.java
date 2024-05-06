package io.kestra.core.serializers;

import static io.kestra.core.utils.Rethrow.throwBiFunction;
import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.*;
import java.util.Optional;
import java.util.function.Consumer;

public final class FileSerde {

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = JacksonMapper.ofIon();
    private static final TypeReference<Object> DEFAULT_TYPE_REFERENCE = new TypeReference<>(){};

    /** The size of the buffer used for reading and writing data from streams. */
    private static final int BUFFER_SIZE = 32 * 1024;

    private FileSerde() {}

    public static void write(OutputStream output, Object row) throws IOException {
        if (row != null) { // avoid writing "null"
            output.write(DEFAULT_OBJECT_MAPPER.writeValueAsBytes(row));
            output.write("\n".getBytes());
        }
    }

    public static Consumer<FluxSink<Object>> reader(BufferedReader input) {
        return s -> {
            String row;

            try {
                while ((row = input.readLine()) != null) {
                    s.next(convert(row));
                }
                s.complete();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T> Consumer<FluxSink<T>> reader(BufferedReader input, Class<T> cls) {
        return s -> {
            String row;

            try {
                while ((row = input.readLine()) != null) {
                    s.next(convert(row, cls));
                }
                s.complete();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static void reader(BufferedReader input, Consumer<Object> consumer) throws IOException {
        String row;
        while ((row = input.readLine()) != null) {
            consumer.accept(convert(row));
        }
    }

    public static boolean reader(BufferedReader input, int maxLines, Consumer<Object> consumer) throws IOException {
        String row;
        int nbLines = 0;
        while ((row = input.readLine()) != null) {
            if (nbLines >= maxLines) {
                return true;
            }

            consumer.accept(convert(row));
            nbLines ++;
        }

        return false;
    }

    private static Object convert(String row) throws JsonProcessingException {
        return DEFAULT_OBJECT_MAPPER.readValue(row, DEFAULT_TYPE_REFERENCE);
    }

    private static <T> T convert(String row, Class<T> cls) throws JsonProcessingException {
        return DEFAULT_OBJECT_MAPPER.readValue(row, cls);
    }

    public static Flux<Object> readAll(Reader reader) throws IOException {
        return readAll(DEFAULT_OBJECT_MAPPER, reader, DEFAULT_TYPE_REFERENCE);
    }

    public static <T> Flux<T> readAll(Reader reader, TypeReference<T> type) throws IOException {
        return readAll(DEFAULT_OBJECT_MAPPER, reader, type);
    }

    public static Flux<Object> readAll(ObjectMapper objectMapper, Reader in) throws IOException {
        return readAll(objectMapper, in, DEFAULT_TYPE_REFERENCE);
    }

    public static <T> Flux<T> readAll(ObjectMapper objectMapper, Reader reader, TypeReference<T> type) throws IOException {
        return Flux.generate(
            () -> createMappingIterator(objectMapper, reader, type),
            throwBiFunction((iterator, sink) -> {
                final T value = iterator.hasNextValue() ? iterator.nextValue() : null;
                Optional.ofNullable(value).ifPresentOrElse(sink::next, sink::complete);
                return iterator;
            }),
            throwConsumer(MappingIterator::close)
        );
    }

    public static <T> Mono<Long> writeAll(Writer writer, Flux<T> values) throws IOException {
        return writeAll(DEFAULT_OBJECT_MAPPER, writer, values);
    }

    public static <T> Mono<Long> writeAll(ObjectMapper objectMapper, Writer writer, Flux<T> values) throws IOException {
        SequenceWriter seqWriter = createSequenceWriter(objectMapper, writer);
        return values
            .doOnNext(throwConsumer(value -> seqWriter.write(value)))
            .doFinally(throwConsumer(ignored -> seqWriter.flush())) // we should have called close() but it generates an exception, so we flush
            .count();
    }

    private static <T> MappingIterator<T> createMappingIterator(ObjectMapper objectMapper, Reader reader, TypeReference<T> type) throws IOException {
        return objectMapper.readerFor(type).readValues(new BufferedReader(reader, BUFFER_SIZE));
    }

    private static SequenceWriter createSequenceWriter(ObjectMapper objectMapper, Writer writer) throws IOException {
        return objectMapper.writer().writeValues(new BufferedWriter(writer, BUFFER_SIZE));
    }
}
