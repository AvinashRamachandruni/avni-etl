package org.avniproject.etl.domain.metadata;

public class Column {
    private static final int POSTGRES_MAX_COLUMN_NAME_LENGTH = 63;
    private static final int NUMBER_OF_CHARACTERS_TO_ACCOMMODATE_HASHCODE = 14;

    private final String name;
    private final Type type;
    private boolean isIndexed;

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, Type type, boolean isIndexed) {
        this.name = name;
        this.type = type;
        this.isIndexed = isIndexed;
    }

    public String getName() {
        if (isColumnNameTruncated()) {
            return String.format("%s (%s)", this.name.substring(0, POSTGRES_MAX_COLUMN_NAME_LENGTH - NUMBER_OF_CHARACTERS_TO_ACCOMMODATE_HASHCODE), Math.abs(this.name.hashCode()));
        }
        return name;
    }

    private boolean isColumnNameTruncated() {
        return this.name.length() > POSTGRES_MAX_COLUMN_NAME_LENGTH;
    }

    public Type getType() {
        return type;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public enum Type {
        text,
        date,
        bool,
        timestamp,
        point,
        timestampWithTimezone,
        numeric,
        time,
        integer;

        public String typeString() {
            switch (this) {
                case integer:
                    return "integer";
                case numeric:
                    return "numeric";
                case date:
                    return "date";
                case time:
                    return "time";
                case text:
                    return "text";
                case bool:
                    return "boolean";
                case point:
                    return "point";
                case timestamp:
                    return "timestamp";
                case timestampWithTimezone:
                    return "timestamp with time zone";
                default:
                    throw new RuntimeException("column_name is not defined for this type"); //Not an expected scenario
            }
        }
    }
}
