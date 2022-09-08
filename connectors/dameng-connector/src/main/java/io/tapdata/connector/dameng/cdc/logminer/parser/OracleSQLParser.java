package io.tapdata.connector.dameng.cdc.logminer.parser;


import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;

@BuildParseTree
public class OracleSQLParser extends BaseParser<Object> {

  static final String COLUMN_NAME_RULE = "ColumnName";
  static final String COLUMN_VALUE_RULE = "ColumnValue";

  Rule Identifier() {
    return Sequence(
      '"',
      Escaped("\""),
      '"'
    );
  }

  Rule ScalarValue() {
    return FirstOf(
      IgnoreCase("NULL"),
      Sequence('\'', Escaped("'"), '\''),
      Escaped("'")
    );
  }

  Rule ColumnValue() {
    return ColumnValueOptions();
  }

  Rule ColumnValueOptions() {
    return FirstOf(EmptyFunc(), Func(), ScalarValue());
  }

  Rule Escaped(String s) {
    return ZeroOrMore(
      FirstOf(
        Sequence(String("'"), s),
        Sequence(TestNot(AnyOf(s)), ANY)
      )
    ).suppressSubnodes();
  }

  Rule Func() {
    return Sequence(
      FuncName(),
      Ch('('),
      Sequence(ColumnValueOptions(), ZeroOrMore(Sequence(WhiteSpace(), Ch(','), WhiteSpace(), ColumnValueOptions()))),
      Ch(')')
    );
  }

  Rule EmptyFunc() {
    return Sequence(FuncName(), Ch('('), Ch(')'));
  }

  Rule TableAliasColname() {
    return FirstOf(
      Sequence(
        OneOrMore(FirstOf(CharRange('A', 'Z'), CharRange('a', 'z'))),
        Ch('.'),
        ColumnName()
      ),
      ColumnName()
    );
  }

  Rule FuncName() {
    return OneOrMore(FirstOf(
      CharRange('0', '9'),
      CharRange('A', 'Z'),
      CharRange('a', 'z'),
      Ch('_'))
    );
  }

  Rule Schema() {
    return Identifier();
  }

  Rule Table() {
    return Identifier();
  }

  Rule ColumnName() {
    return FirstOf(IgnoreCase("ROWID"), Identifier());
  }

  Rule WhiteSpace() {
    return ZeroOrMore(AnyOf(new char[]{' ', '\n', '\t', '\r', '\f'}));
  }

  Rule Insert() {
    return Sequence(
      WhiteSpace(),
      IgnoreCase("INSERT"),
      WhiteSpace(),
      IgnoreCase("INTO"),
      WhiteSpace(),
      Schema(),
      Ch('.'),
      Table(),
      WhiteSpace(),
      Ch('('),
      WhiteSpace(),
      ColumnName(),
      ZeroOrMore(Sequence(WhiteSpace(), Ch(','), WhiteSpace(), ColumnName())),
      WhiteSpace(),
      Ch(')'),
      WhiteSpace(),
      IgnoreCase("VALUES"),
      WhiteSpace(),
      Ch('('),
      WhiteSpace(),
      ColumnValue(),
      ZeroOrMore(Sequence(WhiteSpace(), Ch(','), WhiteSpace(), ColumnValue())),
      Ch(')'),
      WhiteSpace()
    );
  }

  Rule ColumnNameValue() {
    return Sequence(
      WhiteSpace(),
      TableAliasColname(),
      WhiteSpace(),
      FirstOf(Ch('='), IgnoreCase("IS")),
      WhiteSpace(),
      ColumnValue(),
      WhiteSpace()
    );
  }

  Rule MultipleColumnNameValues(String delim) {
    return Sequence(
      WhiteSpace(),
      IgnoreCase(delim),
      WhiteSpace(),
      ColumnNameValue()
    );
  }

  Rule WhereClause() {
    return Optional(Sequence(
        IgnoreCase("WHERE"),
        ColumnNameValue(),
        ZeroOrMore(MultipleColumnNameValues("AND"))
      )
    );
  }

  Rule TableAlias() {
    return ZeroOrMore(
      Sequence(
        WhiteSpace(),
        Sequence(TestNot(IgnoreCase("SET")), TestNot(IgnoreCase("WHERE")), TestNot('(')),
        OneOrMore(FirstOf(CharRange('A', 'Z'), CharRange('a', 'z'))),
        WhiteSpace()
      )
    );
  }

  Rule Update() {
    return Sequence(
      WhiteSpace(),
      IgnoreCase("UPDATE"),
      WhiteSpace(),
      Schema(),
      Ch('.'),
      Table(),
      TableAlias(),
      WhiteSpace(),
      IgnoreCase("SET"),
      ColumnNameValue(),
      ZeroOrMore(MultipleColumnNameValues(",")),
      WhiteSpace(),
      WhereClause()
    );
  }

  Rule Delete() {
    return Sequence(
      WhiteSpace(),
      IgnoreCase("DELETE"),
      WhiteSpace(),
      IgnoreCase("FROM"),
      WhiteSpace(),
      Schema(),
      Ch('.'),
      Table(),
      TableAlias(),
      WhiteSpace(),
      WhereClause()
    );
  }

}
