package io.tapdata.pdk.cli.utils.split;

import java.util.List;

public interface SplitStage<T> {
    List<List<T>> splitToPieces(List<T> data, int eachPieceSize);
}
