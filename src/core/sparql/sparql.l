%option noyywrap

%{
#include <stdio.h>
#include "core/sparql/absyn.hpp"
#include "sparql.tab.h"
%}

%%

[ \t\n]+ 
"_PREDICATE_" {return predicate;}
"FALSE"|"false" {return falsee_;}
"BOUND"|"bound" {return bound;}
"TRUE"|"true" {return truee_;}
"ORDER BY"|"order by" {return order_by;}
"FILTER"|"filter" {return filter;}
"COUNT"|"count" {return count;}
"UNION"|"union" {return union_;}
"OPTIONAL"|"optional" {return optional_;}
"PREFIX"|"prefix" {return prefix;}
"SELECT"|"select" {return select_;}
"ASK"|"ask" {return ask_;}
"WHERE"|"where" {return where;}
"FROM"|"from" {return from;}
"SNAPSHOT"|"snapshot" {return snapshot;}
"LIMIT"|"limit" {return limit;}
"OFFSET"|"offset" {return offset;}
"ASC"|"asc" {return asc;}
"DESC"|"desc" {return desc;}
"||" {return or_;}
"&&" {return and_;}
"(" {return lparen;}
")" {return rparen;}
"{" {return lcurly;}
"}" {return rcurly;}
(19[0-9]{2}|[2-9][0-9]{3})-((0(1|3|5|7|8)|10|12)-(0[1-9]|1[0-9]|2[0-9]|3[0-1])|(0(4|6|9)|11)-(0[1-9]|1[0-9]|2[0-9]|30)|(02)-(0[1-9]|1[0-9]|2[0-9]))T(0[0-9]|1[0-9]|2[0-3])(:[0-5][0-9]){2} {yylval.str = yytext; return datetime;}
":" {return colon;}
"[" {return lbracket;}
\[[ \t\n\r\f]*\] {return anon;}
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
[?$][a-zA-Z0-9]+ {yylval.str=yytext; return variable;}
\<[^\<\>\\\`\{\}\^\|\"T]+\> {yylval.str=yytext; return iri;}
[a-zA-Z][a-zA-Z0-9]* {yylval.str=yytext; return identifier;}
-?[0-9]+ {yylval.str = yytext;return integer;}
-?([0-9]+\.[0-9]*)|\.[0-9]+ {yylval.str = yytext;return decimal;}

%%