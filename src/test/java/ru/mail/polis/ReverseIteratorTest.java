package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ReverseIteratorTest extends TestBase {
    @Test
    public void emptyIterator(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(randomKey(), randomValue());

            final Iterator<Record> iterator = dao.reverseIterator(ByteBuffer.allocate(0));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void singleIteration(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            final ByteBuffer key = randomKey();
            final ByteBuffer value = randomValue();
            dao.upsert(key, value);

            Record record = dao.reverseIterator(key).next();

            assertEquals(key, record.getKey());
            assertEquals(value, record.getValue());
        }
    }

    @Test
    public void iterateManyRecords(@TempDir File data) throws IOException {
        final NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        ByteBuffer key = ByteBuffer.allocate(0);

        for (int i = 0; i < 1_000; i++) {
            if (i == 500) {
                key = randomKey();
                map.put(key, randomValue());
            } else
                map.put(randomKey(), randomValue());
        }

        try (DAO dao = DAOFactory.create(data)) {
            for (var entry : map.entrySet())
                dao.upsert(entry.getKey(), entry.getValue());

            final Iterator<Record> iterator = dao.reverseIterator(key);

            for (var entry : map.headMap(key, true).descendingMap().entrySet()) {
                final Record record = iterator.next();

                assertEquals(entry.getKey(), record.getKey());
                assertEquals(entry.getValue(), record.getValue());
            }
        }
    }

    @Test
    public void reverseFullIterator(@TempDir File data) throws IOException {
        final NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

        for (int i = 0; i < 1_000; i++)
            map.put(randomKey(), randomValue());

        try (DAO dao = DAOFactory.create(data)) {
            for (var entry : map.entrySet())
                dao.upsert(entry.getKey(), entry.getValue());

            Iterator<Record> iterator = dao.reverseIterator(map.lastKey());

            for (var entry : map.descendingMap().entrySet()) {
                final Record record = iterator.next();

                assertEquals(entry.getKey(), record.getKey());
                assertEquals(entry.getValue(), record.getValue());
            }
        }
    }
}
