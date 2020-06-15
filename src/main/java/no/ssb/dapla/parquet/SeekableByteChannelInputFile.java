package no.ssb.dapla.parquet;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;

class SeekableByteChannelInputFile implements InputFile {

    private final SeekableByteChannel input;

    SeekableByteChannelInputFile(SeekableByteChannel input) {
        this.input = Objects.requireNonNull(input);
    }

    @Override
    public long getLength() throws IOException {
        return input.size();
    }

    @Override
    public SeekableInputStream newStream() {
        return new DelegatingSeekableInputStream(Channels.newInputStream(input)) {
            @Override
            public long getPos() throws IOException {
                return input.position();
            }

            @Override
            public void seek(long newPos) throws IOException {
                input.position(newPos);
            }
        };
    }
}
