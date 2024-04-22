package io.tapdata.pdk.cli.utils.split;

import io.tapdata.pdk.cli.utils.PrintUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SplitByFileSizeImpl implements SplitStage<File> {
    final PrintUtil log;
    final int partitionBatch;
    public SplitByFileSizeImpl(int partitionBatch, PrintUtil log) {
        if (partitionBatch <= 0) {
            partitionBatch = 1;
        }
        this.partitionBatch = partitionBatch;
        this.log = log;
    }
    @Override
    public List<List<File>> splitToPieces(List<File> data, int eachPieceSize) {
        log.print(PrintUtil.TYPE.DEBUG, String.format("- Starting to perform fuzzy equalization based on the memory size of the Jar package, with a number of batches: %s", partitionBatch));
        long start = System.currentTimeMillis();
        try {
            AtomicLong totalMemorySize = new AtomicLong();
            List<File> sortedFiles = data.stream().filter(Objects::nonNull).filter(file -> {
                totalMemorySize.addAndGet(file.length());
                return true;
            }).sorted((f1, f2) -> {
                long length1 = f1.length();
                long length2 = f2.length();
                return Long.compare(length1, length2);
            }).collect(Collectors.toList());
            long partitionSize = totalMemorySize.get() / partitionBatch;
            log.print(PrintUtil.TYPE.DEBUG, String.format("- Number of Jar packages submitted: %s, total memory: %s bytes, expected batch: %s bytes", data.size(), totalMemorySize.get(), partitionSize));
            return foreachAllFiles(sortedFiles, partitionSize);
        } finally {
            log.print(PrintUtil.TYPE.DEBUG, String.format("- Perform fuzzy equalization based on the memory size of the Jar package completed, with a number of batches: %s, cost time: %s", partitionBatch, PrintUtil.formatDate(start)));
        }
    }

    protected List<List<File>> foreachAllFiles(List<File> sortedFiles, long partitionSize) {
        List<List<File>> files = new ArrayList<>();
        Set<Integer> removedFiles = new HashSet<>();
        List<File> minSizeList = null;
        long minSize = 0;
        int location = 0;
        while (location != partitionBatch) {
            location ++;
            List<File> partition = new ArrayList<>();
            log.print(PrintUtil.TYPE.DEBUG, String.format("- Starting to allocate Jar packets to the %s batch of lists, the current list requires a total of approximately %s bytes of Jar packets to be allocated", location,  partitionSize));
            long currentSize = 0;
            int size = sortedFiles.size();
            for (int index = 0; index < size; index++) {
                if (removedFiles.contains(index)) {
                    continue;
                }
                File file = sortedFiles.get(index);
                String name = file.getName();
                long length = file.length();
                long expectedSize = currentSize + length;
                if (expectedSize > partitionSize) {
                    log.print(PrintUtil.TYPE.DEBUG, String.format("- %s memory usage: %s byte, %s Jar packages (%s byte) have been allocated in the %s batch list, exceeding the expected %s bytes after addition",
                            name, length, partition.size(), currentSize, location, partitionSize));
                    if (partition.isEmpty()) {
                        log.print(PrintUtil.TYPE.DEBUG, String.format("- Directly add %s to the %s batch list, where Jar packages have not been assigned yet. After joining, the current list will end with Jar package allocation",
                                name, location));
                        partition.add(file);
                        currentSize += length;
                        removedFiles.add(index);
                        break;
                    }

                    log.print(PrintUtil.TYPE.DEBUG, String.format("- Start traversing unallocated Jar packages from small to large in terms of memory usage, need to find the Jar package with the highest and most suitable memory usage in the %s batch of lists", name));
                    File lastAppropriateGoals = null;
                    Integer lastAppropriateGoalsIndex = null;
                    for (int indexReverse = size-1; indexReverse > index; indexReverse--) {
                        if (removedFiles.contains(index)) {
                            continue;
                        }
                        file = sortedFiles.get(indexReverse);
                        length = file.length();
                        expectedSize = currentSize + length;
                        if (expectedSize > partitionSize) {
                            if (null != lastAppropriateGoals) {
                                log.print(PrintUtil.TYPE.DEBUG, String.format("- Traversing unallocated Jar packages from small to large in terms of memory usage completed in the %s batch of lists, connector: %s, memory usage: %s bytes",
                                        location, file.getName(), length));
                                partition.add(file);
                                removedFiles.add(lastAppropriateGoalsIndex);
                            }
                            break;
                        }
                        if (expectedSize <= partitionSize) {
                            lastAppropriateGoals = file;
                            currentSize += length;
                            lastAppropriateGoalsIndex = indexReverse;
                        }
                        if (expectedSize == partitionSize) {
                            log.print(PrintUtil.TYPE.DEBUG, String.format("- Traversing unallocated Jar packages from small to large in terms of memory usage completed in the %s batch of lists, connector: %s, memory usage: %s bytes",
                                    location, file.getName(), length));
                            partition.add(file);
                            removedFiles.add(indexReverse);
                            break;
                        }
                    }
                    continue;
                }

                removedFiles.add(index);
                log.print(PrintUtil.TYPE.DEBUG, String.format("- %s memory usage: %s byte, %s Jar packages (%s byte) have been allocated in the first batch of list, %s expected to require %s bytes, suitable for inclusion in the list",
                        name,
                        length,
                        partition.size(),
                        currentSize,
                        expectedSize == partitionSize ? String.format("and after addition, they match the expected %s bytes,", partitionSize) : "",
                        partitionSize));
                partition.add(file);
                currentSize += length;
                if (expectedSize == partitionSize) {
                    break;
                }
            }

            files.add(partition);
            if (null == minSizeList || minSize > currentSize) {
                minSize = currentSize;
                minSizeList = partition;
            }
            log.print(PrintUtil.TYPE.DEBUG, String.format("- The allocation of Jar packages in the %s batch of lists has been completed. The current list requires a total of %s Jar packages that have been allocated, with a total of %s bytes allocated", location, partition.size(), currentSize));
        }
        if (removedFiles.size() != sortedFiles.size()) {
            for (int index = 0; index < sortedFiles.size(); index++) {
                if (!removedFiles.contains(index)) {
                    assert minSizeList != null;
                    minSizeList.add(sortedFiles.get(index));
                }
            }
        }
        return files;
    }
}
