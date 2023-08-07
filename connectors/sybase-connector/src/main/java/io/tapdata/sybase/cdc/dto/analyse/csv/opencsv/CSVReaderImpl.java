package io.tapdata.sybase.cdc.dto.analyse.csv.opencsv;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author GavinXiao
 * @description CSVReaderImpl create by Gavin
 * @create 2023/7/31 16:16
 **/
public class CSVReaderImpl extends CSVReader {
    private BufferedReader br;
    private boolean hasNext;
    private CSVParserImpl parser;
    private int skipLines;
    private boolean linesSkiped;
    public static final int DEFAULT_SKIP_LINES = 0;
    Log log;
    public void setLog(Log log){
        this.log = log;
        this.parser.setLog(log);
    }
    public CSVReaderImpl(Reader reader, List<SpecialField> specialFields) {
        this(reader, ',', '"', '\\', specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, List<SpecialField> specialFields) {
        this(reader, separator, '"', '\\', specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, '\\', 0, false, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, boolean strictQuotes, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, '\\', 0, strictQuotes, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, char escape, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, escape, 0, false, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, int line, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, '\\', line, false, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, char escape, int line, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, escape, line, false, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes, List<SpecialField> specialFields) {
        this(reader, separator, quotechar, escape, line, strictQuotes, true, specialFields);
    }

    public CSVReaderImpl(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes, boolean ignoreLeadingWhiteSpace, List<SpecialField> specialFields) {
        super(reader, separator, quotechar, escape, line, strictQuotes, ignoreLeadingWhiteSpace);
        this.hasNext = true;
        this.br = new BufferedReader(reader);
        this.parser = new CSVParserImpl(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace);
        this.parser.specialFields(specialFields);
        this.skipLines = line;
    }

    public List<String[]> readAll() throws IOException {
        ArrayList allElements = new ArrayList();

        while(this.hasNext) {
            String[] nextLineAsTokens = this.readNext();
            if (nextLineAsTokens != null) {
                allElements.add(nextLineAsTokens);
            }
        }

        return allElements;
    }

    public String[] readNext() throws IOException {
        String[] result = null;

        do {
            String nextLine = this.getNextLine();
            if (!this.hasNext) {
                return result;
            }

            String[] r = this.parser.parseLineMulti(nextLine);
            if (r.length > 0) {
                if (result == null) {
                    result = r;
                } else {
                    String[] t = new String[result.length + r.length];
                    System.arraycopy(result, 0, t, 0, result.length);
                    System.arraycopy(r, 0, t, result.length, r.length);
                    result = t;
                }
            }
        } while(this.parser.isPending());

        return result;
    }

    private String getNextLine() throws IOException {
        if (!this.linesSkiped) {
            for(int i = 0; i < this.skipLines; ++i) {
                this.br.readLine();
            }

            this.linesSkiped = true;
        }

        String nextLine = this.br.readLine();
        if (nextLine == null) {
            this.hasNext = false;
        }

        return this.hasNext ? nextLine : null;
    }

    public void close() throws IOException {
        this.br.close();
    }
}
