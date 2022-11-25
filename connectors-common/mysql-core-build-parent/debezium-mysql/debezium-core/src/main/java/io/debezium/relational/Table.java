/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;

/**
 * An immutable definition of a table.
 */
@Immutable
public interface Table {
    /**
     * Obtain an table definition editor that can be used to define a table.
     *
     * @return the editor; never null
     */
    static TableEditor editor() {
        return new TableEditorImpl();
    }

    /**
     * Get the identifier for this table.
     * @return the identifier; never null
     */
    TableId id();

    /**
     * The list of column names that make up the primary key for this table.
     *
     * @return the immutable list of column names that make up the primary key; never null but possibly empty
     */
    List<String> primaryKeyColumnNames();

    /**
     * Get the columns that make up the primary key for this table.
     * @return the immutable list of columns that make up the primary key; never null but possibly empty
     */
    default List<Column> primaryKeyColumns() {
        List<Column> pkColumns = primaryKeyColumnNames()
                .stream()
                .map(this::columnWithName)
                .collect(Collectors.toList());

        return Collections.unmodifiableList(pkColumns);
    }

    /**
     * Utility to obtain a copy of a list of the columns that satisfy the specified predicate.
     * @param predicate the filter predicate; may not be null
     * @return the list of columns that satisfy the predicate; never null but possibly empty
     */
    default List<Column> filterColumns(Predicate<Column> predicate) {
        return columns()
                .stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * The list of column names that make up this table.
     * <p>
     * Note: If feasible, call {@link #columns()} instead, e.g. if just interested in the number of columns.
     *
     * @return the immutable list of column names ; never null but possibly empty
     */
    List<String> retrieveColumnNames();

    /**
     * Get the definitions for the columns in this table, in the same order in which the table defines them.
     *
     * @return the immutable and ordered list of definitions; never null
     */
    List<Column> columns();

    /**
     * Get the definition for the column in this table with the supplied name. The case of the supplied name does not matter.
     *
     * @param name the case-insensitive name of the column
     * @return the column definition, or null if there is no column with the given name
     */
    Column columnWithName(String name);

    /**
     * Get the database-specific name of the default character set used by columns in this table.
     *
     * @return the database-specific character set name used by default in columns of this table, or {@code null} if there is no
     * such default character set name defined on the table
     */
    String defaultCharsetName();

    /**
     * Determine if the named column is part of the primary key.
     *
     * @param columnName the name of the column
     * @return {@code true} if a column exists in this table and it is part of the primary key, or {@code false} otherwise
     */
    default boolean isPrimaryKeyColumn(String columnName) {
        Column column = columnWithName(columnName);
        return column == null ? false : primaryKeyColumnNames().contains(column.name());
    }

    /**
     * Determine if the named column is auto-incremented.
     *
     * @param columnName the name of the column
     * @return {@code true} if a column exists in this table and it is auto-incremented, or {@code false} otherwise
     */
    default boolean isAutoIncremented(String columnName) {
        Column column = columnWithName(columnName);
        return column == null ? false : column.isAutoIncremented();
    }

    /**
     * Determine if the values in the named column is generated by the database.
     *
     * @param columnName the name of the column
     * @return {@code true} if a column exists in this table and its values are generated, or {@code false} otherwise
     */
    default boolean isGenerated(String columnName) {
        Column column = columnWithName(columnName);
        return column == null ? false : column.isGenerated();
    }

    /**
     * Determine if the values in the named column is optional.
     *
     * @param columnName the name of the column
     * @return {@code true} if a column exists in this table and is optional, or {@code false} otherwise
     */
    default boolean isOptional(String columnName) {
        Column column = columnWithName(columnName);
        return column == null ? false : column.isOptional();
    }

    /**
     * Obtain an editor that contains the same information as this table definition.
     *
     * @return the editor; never null
     */
    TableEditor edit();
}
