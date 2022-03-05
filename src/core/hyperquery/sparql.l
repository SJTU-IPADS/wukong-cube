%option yylineno

%{
#include <stdio.h>
#include "core/common/errors.hpp"
#include "core/hyperquery/absyn.hpp"
#include "sparql.tab.h"
%}

%%

[ \t\n]+ 
"PREFIX"|"prefix" {return prefix;}
"SELECT"|"select" {return select_;}
"WHERE"|"where" {return where;}
"buildin" {return buildin;}
"etype" {return etype;}
"vtype" {return vtype;}
"edges" {return edges;}
"vertices" {return vertices;}
"intersectEdges" {return intersect_edges;}
"containEdges" {return contain_edges;}
"inEdges" {return in_edges;}
"intersectVertices" {return intersect_vertices;}
"||" {return or_;}
"&&" {return and_;}
"(" {return lparen;}
")" {return rparen;}
"{" {return lcurly;}
"}" {return rcurly;}
":" {return colon;}
"[" {return lbracket;}
"*" {return mul_;}
"+" {return plus_;}
"-" {return minus_;}
"^^" {return type;}
"]" {return rbracket;}
"." {return dot;}
"," {return comma;}
"@" {return at;}
"%" {return percent;}
"<-" {return larrow;}
"->" {return rarrow;}
"=" {return equal;}
"!=" {return not_equal;}
"!" {return not_;}
"<" {return less;}
"<=" {return less_or_equal;} 
">" {return greater;} 
">=" {return greater_or_equal;} 
\"([^\"])+\" {yylval.str=yytext; return string_;}
[?$][a-zA-Z0-9_]+ {yylval.str=yytext; return variable;}
\<[^\<\>\\\`\{\}\^\|\"]+\> {yylval.str=yytext; return iri;}
[a-zA-Z][a-zA-Z0-9]* {yylval.str=yytext; return identifier;}
-?[0-9]+ {yylval.str = yytext;return integer;}
-?([0-9]+\.[0-9]*)|\.[0-9]+ {yylval.str = yytext;return decimal;}

%%

int yywrap(void) {
    BEGIN INITIAL;
    return 1;
}

void yyerror(std::string error) {
    yywrap();
    logstream(LOG_ERROR) << "line " << yylineno << ": " << error << LOG_endl;
    throw wukong::WukongException(wukong::SYNTAX_ERROR);
}