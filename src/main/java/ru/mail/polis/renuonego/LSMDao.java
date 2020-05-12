package ru.mail.polis.renuonego;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

@SuppressWarnings("ConstantConditions")
public final class LSMDao implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static final String PREFIX = "SSTABLE";

    private Table memTable = new MemTable();
    private final File base;
    private int generation;
    private final long flushThreshold;
    private final Collection<SSTable> ssTables;

    /**
     * Creates LSM Dao.
     *
     * @param base           is directory with SSTables
     * @param flushThreshold is threshold of MemTable's size
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(@NotNull final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        ssTables = new ArrayList<>();

        final var options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDepth = 1;

        Files.walkFileTree(base.toPath(), options, maxDepth, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
                if (path.getFileName().toString().endsWith(SUFFIX)
                        && path.getFileName().toString().startsWith(PREFIX)) {
                    ssTables.add(new SSTable(path.toFile()));
                }
                return FileVisitResult.CONTINUE;
            }
        });

        generation = ssTables.size() + 1;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(cellIterator(from),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @NotNull
    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) throws IOException {

        final List<Iterator<Cell>> filesIterators = new ArrayList<>();

        for (final SSTable ssTable : ssTables) {
            filesIterators.add(ssTable.iterator(from));
        }

        filesIterators.add(memTable.iterator(from));
        @SuppressWarnings("UnstableApiUsage") final Iterator<Cell> mergedCells = Iterators.mergeSorted(filesIterators, Cell.COMPARATOR);
        final Iterator<Cell> cells = Iters.collapseEquals(mergedCells, Cell::getKey);

        return Iterators.filter(cells, cell -> !cell.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold) flush();
    }

    private void flush() throws IOException {
        final File tmp = new File(base, PREFIX + generation + TEMP);
        SSTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);

        final File dest = new File(base, PREFIX + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.add(new SSTable(dest));

        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() >= flushThreshold) flush();
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() != 0) {
            flush();
        }

        for (final SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }
}
