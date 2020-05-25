package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ReverseIteratorTest extends TestBase {

    @Test
    public void emptyIterator(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            ByteBuffer key;
            do key = randomKey(); while (key == ByteBuffer.allocate(0));

            dao.upsert(key, randomValue());

            final Iterator<Record> iterator = dao.reverseIterator(ByteBuffer.allocate(0));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void nonExistentKeyIteration(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            final ByteBuffer zero = ByteBuffer.allocate(0);
            final ByteBuffer value = randomValue();
            dao.upsert(zero, value);

            final Iterator<Record> iterator = dao.reverseIterator(ByteBuffer.allocate(100));

            final Record next = iterator.next();
            assertEquals(next.getKey(), zero);
            assertEquals(next.getValue(), value);
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

        final int size = 500;
        for (int i = 0; i < size; i++) {
            if (i == size / 2) {
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

        final int size = 500;
        for (int i = 0; i < size; i++)
            map.put(randomKey(), randomValue());

        try (DAO dao = DAOFactory.create(data)) {
            for (var entry : map.entrySet())
                dao.upsert(entry.getKey(), entry.getValue());

            Iterator<Record> iterator = dao.reverseIterator();

            for (var entry : map.descendingMap().entrySet()) {
                final Record record = iterator.next();

                assertEquals(entry.getKey(), record.getKey());
                assertEquals(entry.getValue(), record.getValue());
            }
        }
    }

    @Test
    void reverseIteratorWithCompact(@TempDir File data) throws IOException {
        final int keyCount = 10;
        final int overwrites = 10;


        final ByteBuffer value = randomValue();
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);

        for (int i = 0; i < keyCount; i++)
            keys.add(randomKey());

        for (int round = 0; round < overwrites; round++) {
            try (DAO dao = DAOFactory.create(data)) {
                for (final ByteBuffer key : keys) {
                    dao.upsert(key, join(key, value));
                }
            }
        }

        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();

            final Iterator<Record> reverseIterator = dao.reverseIterator();
            while (reverseIterator.hasNext()) {
                final Record next = reverseIterator.next();
                assertEquals(next.getValue(), dao.get(next.getKey()));
            }
        }
    }

    @Test
    void reverseIteratorFlush(@TempDir File data) throws IOException {
        final NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

        final int size = 500;
        for (int i = 0; i < size; i++)
            map.put(randomKey(), randomValue());

        DAO dao = DAOFactory.create(data);

        for (int i = 0; i < size; i++)
            dao.upsert(randomKey(), randomValue());

        dao.close();

        Iterator<Record> iterator = dao.reverseIterator();

        for (var entry : map.descendingMap().entrySet()) {
            final Record record = iterator.next();

            assertNotEquals(entry.getKey(), record.getKey());
            assertNotEquals(entry.getValue(), record.getValue());
        }
    }

}
