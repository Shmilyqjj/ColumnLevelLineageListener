package top.qjj.shmily.lineage.antlr4.spark.sql;
import top.qjj.shmily.lineage.antlr4.spark.sql.gen.sparkSQLLexer;
import top.qjj.shmily.lineage.antlr4.spark.sql.gen.sparkSQLParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;


/**
 * :Description: Analyse sparkSQL by antlr4.
 * :Author: 佳境Shmily
 * :Create Time: 2021/2/3 17:07
 * :Site: shmily-qjj.top
 */
public class Analyzer {
    public static void main(String[] args) {
        ANTLRInputStream input = new ANTLRInputStream("SELECT * FROM AAA A");
        sparkSQLLexer lexer = new sparkSQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        sparkSQLParser parser = new sparkSQLParser(tokens);
        parser.setBuildParseTree(true);
        ParseTree tree = parser.statement();
        ParseTreeWalker walker = new ParseTreeWalker();
        System.out.println(tree);
    }
}
