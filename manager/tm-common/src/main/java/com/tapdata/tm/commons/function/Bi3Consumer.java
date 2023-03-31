package com.tapdata.tm.commons.function;

import java.util.Objects;

@FunctionalInterface
public interface Bi3Consumer<T, R, U> {

  void accept(T t, R r, U u);

  /**
   * Returns a composed {@code BiConsumer} that performs, in sequence, this
   * operation followed by the {@code after} operation. If performing either
   * operation throws an exception, it is relayed to the caller of the
   * composed operation.  If performing this operation throws an exception,
   * the {@code after} operation will not be performed.
   *
   * @param after the operation to perform after this operation
   * @return a composed {@code BiConsumer} that performs in sequence this
   * operation followed by the {@code after} operation
   * @throws NullPointerException if {@code after} is null
   */
  default Bi3Consumer<T, R, U> andThen(Bi3Consumer<? super T, ? super R, ? super U> after) {
    Objects.requireNonNull(after);

    return (l, r, u) -> {
      accept(l, r, u);
      after.accept(l, r, u);
    };
  }
}
