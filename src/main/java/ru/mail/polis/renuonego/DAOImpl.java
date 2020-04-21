package ru.mail.polis.renuonego;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {

    private final SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> Record.of(Objects.requireNonNull(e).getKey(), e.getValue())
        );
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        // do nothing because data in memory
    }
}
