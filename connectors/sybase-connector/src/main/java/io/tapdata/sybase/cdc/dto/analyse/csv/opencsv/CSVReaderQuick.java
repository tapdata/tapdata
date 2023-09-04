package io.tapdata.sybase.cdc.dto.analyse.csv.opencsv;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import au.com.bytecode.opencsv.CSVReader;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A very simple CSV reader released under a commercial-friendly license.
 * 
 * @author Glen Smith
 * 
 */
public class CSVReaderQuick extends CSVReader {

    private BufferedReader br;

    private boolean hasNext = true;

    private CSVParserQuicker parser;
    
    private int skipLines;

    private boolean linesSkiped;

    Iterator<String> iterator;
    Log log;

    public void setLog(Log log) {
        this.log = log;
        this.parser.setLog(log);
    }

    /**
     * The default line to start reading.
     */
    public static final int DEFAULT_SKIP_LINES = 0;

    /**
     * Constructs CSVReader using a comma for the separator.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     */
    public CSVReaderQuick(Reader reader) {
        this(reader, CSVParserQuicker.DEFAULT_SEPARATOR, CSVParserQuicker.DEFAULT_QUOTE_CHARACTER, CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVReader with supplied separator.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries.
     */
    public CSVReaderQuick(Reader reader, char separator) {
        this(reader, separator, CSVParserQuicker.DEFAULT_QUOTE_CHARACTER, CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar) {
        this(reader, separator, quotechar, CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES, CSVParserQuicker.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader with supplied separator, quote char and quote handling
     * behavior.
     *
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param strictQuotes
     *            sets if characters outside the quotes are ignored
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar, boolean strictQuotes) {
        this(reader, separator, quotechar, CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES, strictQuotes);
    }

   /**
     * Constructs CSVReader with supplied separator and quote char.
     *
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escape
     *            the character to use for escaping a separator or quote
     */

    public CSVReaderQuick(Reader reader, char separator,
			char quotechar, char escape) {
        this(reader, separator, quotechar, escape, DEFAULT_SKIP_LINES, CSVParserQuicker.DEFAULT_STRICT_QUOTES);
	}
    
    /**
     * Constructs CSVReader with supplied separator and quote char.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param line
     *            the line number to skip for start reading 
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar, int line) {
        this(reader, separator, quotechar, CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER, line, CSVParserQuicker.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     *
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escape
     *            the character to use for escaping a separator or quote
     * @param line
     *            the line number to skip for start reading
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar, char escape, int line) {
        this(reader, separator, quotechar, escape, line, CSVParserQuicker.DEFAULT_STRICT_QUOTES);
    }
    
    /**
     * Constructs CSVReader with supplied separator and quote char.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escape
     *            the character to use for escaping a separator or quote
     * @param line
     *            the line number to skip for start reading
     * @param strictQuotes
     *            sets if characters outside the quotes are ignored
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes) {
        this(reader, separator, quotechar, escape, line, strictQuotes, CSVParserQuicker.DEFAULT_IGNORE_LEADING_WHITESPACE);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     * 
     * @param reader
     *            the reader to an underlying CSV source.
     * @param separator
     *            the delimiter to use for separating entries
     * @param quotechar
     *            the character to use for quoted elements
     * @param escape
     *            the character to use for escaping a separator or quote
     * @param line
     *            the line number to skip for start reading
     * @param strictQuotes
     *            sets if characters outside the quotes are ignored
     * @param ignoreLeadingWhiteSpace
     *            it true, parser should ignore white space before a quote in a field
     */
    public CSVReaderQuick(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes, boolean ignoreLeadingWhiteSpace) {
        super(reader, separator, quotechar, escape, line, strictQuotes, ignoreLeadingWhiteSpace);
        this.br = new BufferedReader(reader);
        this.parser = new CSVParserQuicker(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace);
        this.skipLines = line;
    }



    public CSVReaderQuick(Reader reader, List<String> lines, int line) {
        super(reader,
                CSVParserQuicker.DEFAULT_SEPARATOR,
                CSVParserQuicker.DEFAULT_QUOTE_CHARACTER,
                CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER,
                CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER,
                CSVParserQuicker.DEFAULT_STRICT_QUOTES,
                CSVParserQuicker.DEFAULT_IGNORE_LEADING_WHITESPACE);
        this.hasNext = true;
        if (null == lines) {
            this.br = new BufferedReader(reader);
        } else {
            this.iterator = lines.iterator();
        }
        this.parser = new CSVParserQuicker(
                CSVParserQuicker.DEFAULT_SEPARATOR,
                CSVParserQuicker.DEFAULT_QUOTE_CHARACTER,
                CSVParserQuicker.DEFAULT_ESCAPE_CHARACTER,
                CSVParserQuicker.DEFAULT_STRICT_QUOTES,
                CSVParserQuicker.DEFAULT_IGNORE_LEADING_WHITESPACE);
        this.skipLines = line;
    }

   /**
     * Reads the entire file into a List with each element being a String[] of
     * tokens.
     * 
     * @return a List of String[], with each String[] representing a line of the
     *         file.
     * 
     * @throws IOException
     *             if bad things happen during the read
     */
    public List<String[]> readAll() throws IOException {

        List<String[]> allElements = new ArrayList<String[]>();
        while (hasNext) {
            String[] nextLineAsTokens = readNext();
            if (nextLineAsTokens != null)
                allElements.add(nextLineAsTokens);
        }
        return allElements;

    }

    /**
     * Reads the next line from the buffer and converts to a string array.
     * 
     * @return a string array with each comma-separated element as a separate
     *         entry.
     * 
     * @throws IOException
     *             if bad things happen during the read
     */
    public String[] readNext() throws IOException {
    	
    	String[] result = null;
    	do {
    		String nextLine = getNextLine();
    		if (!hasNext) {
    			return result; // should throw if still pending?
    		}
    		String[] r = parser.parseLineMulti(nextLine);
    		if (r.length > 0) {
    			if (result == null) {
    				result = r;
    			} else {
    				String[] t = new String[result.length+r.length];
    				System.arraycopy(result, 0, t, 0, result.length);
    				System.arraycopy(r, 0, t, result.length, r.length);
    				result = t;
    			}
    		}
    	} while (parser.isPending());
    	return result;
    }

    /**
     * Reads the next line from the file.
     * 
     * @return the next line from the file without trailing newline
     * @throws IOException
     *             if bad things happen during the read
     */
    private String getNextLine() throws IOException {
    	if (!this.linesSkiped) {
            for (int i = 0; i < skipLines; i++) {
                if (null != iterator) {
                    getLine();
                } else {
                    this.br.readLine();
                }
            }
            this.linesSkiped = true;
        }
        String nextLine = br.readLine();
        if (nextLine == null) {
            hasNext = false;
        }
        return hasNext ? nextLine : null;
    }

    /**
     * Closes the underlying reader.
     * 
     * @throws IOException if the close fails
     */
    public void close() throws IOException {
        if (null == iterator) {
            this.br.close();
        }
    }

    private String getLine() {
        return null != iterator && iterator.hasNext() ? iterator.next() : null;
    }
}
