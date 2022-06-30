package io.tapdata.pdk.core.constants;

/**
 * <P>The class that defines the constants that are used to identify generic
 * SQL types, called JDBC types.
 * <p>
 * This class is never instantiated.
 */
public interface Types {

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>BIT</code>.
 */
        int BIT             =  -7;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>TINYINT</code>.
 */
        int TINYINT         =  -6;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>SMALLINT</code>.
 */
        int SMALLINT        =   5;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>INTEGER</code>.
 */
        int INTEGER         =   4;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>BIGINT</code>.
 */
        int BIGINT          =  -5;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>FLOAT</code>.
 */
        int FLOAT           =   6;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>REAL</code>.
 */
        int REAL            =   7;


/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>DOUBLE</code>.
 */
        int DOUBLE          =   8;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>NUMERIC</code>.
 */
        int NUMERIC         =   2;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>DECIMAL</code>.
 */
        int DECIMAL         =   3;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>CHAR</code>.
 */
        int CHAR            =   1;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>VARCHAR</code>.
 */
        int VARCHAR         =  12;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>LONGVARCHAR</code>.
 */
        int LONGVARCHAR     =  -1;


/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>DATE</code>.
 */
        int DATE            =  91;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>TIME</code>.
 */
        int TIME            =  92;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>TIMESTAMP</code>.
 */
        int TIMESTAMP       =  93;


/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>BINARY</code>.
 */
        int BINARY          =  -2;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>VARBINARY</code>.
 */
        int VARBINARY       =  -3;

/**
 * <P>The constant in the Java programming language, sometimes referred
 * to as a type code, that identifies the generic SQL type
 * <code>LONGVARBINARY</code>.
 */
        int LONGVARBINARY   =  -4;

/**
 * <P>The constant in the Java programming language
 * that identifies the generic SQL value
 * <code>NULL</code>.
 */
        int NULL            =   0;

    /**
     * The constant in the Java programming language that indicates
     * that the SQL type is database-specific and
     * gets mapped to a Java object that can be accessed via
     * the methods <code>getObject</code> and <code>setObject</code>.
     */
        int OTHER           = 1111;



    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>JAVA_OBJECT</code>.
     * @since 1.2
     */
        int JAVA_OBJECT         = 2000;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>DISTINCT</code>.
     * @since 1.2
     */
        int DISTINCT            = 2001;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>STRUCT</code>.
     * @since 1.2
     */
        int STRUCT              = 2002;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>ARRAY</code>.
     * @since 1.2
     */
        int ARRAY               = 2003;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>BLOB</code>.
     * @since 1.2
     */
        int BLOB                = 2004;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>CLOB</code>.
     * @since 1.2
     */
        int CLOB                = 2005;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * <code>REF</code>.
     * @since 1.2
     */
        int REF                 = 2006;

    /**
     * The constant in the Java programming language, somtimes referred to
     * as a type code, that identifies the generic SQL type <code>DATALINK</code>.
     *
     * @since 1.4
     */
    int DATALINK = 70;

    /**
     * The constant in the Java programming language, somtimes referred to
     * as a type code, that identifies the generic SQL type <code>BOOLEAN</code>.
     *
     * @since 1.4
     */
    int BOOLEAN = 16;

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>ROWID</code>
     *
     * @since 1.6
     *
     */
    int ROWID = -8;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCHAR</code>
     *
     * @since 1.6
     */
    int NCHAR = -15;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NVARCHAR</code>.
     *
     * @since 1.6
     */
    int NVARCHAR = -9;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
     *
     * @since 1.6
     */
    int LONGNVARCHAR = -16;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>NCLOB</code>.
     *
     * @since 1.6
     */
    int NCLOB = 2011;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type <code>XML</code>.
     *
     * @since 1.6
     */
    int SQLXML = 2009;

    //--------------------------JDBC 4.2 -----------------------------

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type {@code REF CURSOR}.
     *
     * @since 1.8
     */
    int REF_CURSOR = 2012;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIME WITH TIMEZONE}.
     *
     * @since 1.8
     */
    int TIME_WITH_TIMEZONE = 2013;

    /**
     * The constant in the Java programming language, sometimes referred to
     * as a type code, that identifies the generic SQL type
     * {@code TIMESTAMP WITH TIMEZONE}.
     *
     * @since 1.8
     */
    int TIMESTAMP_WITH_TIMEZONE = 2014;

}
