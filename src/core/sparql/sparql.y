%{
#include <stdio.h>
#include <stdlib.h>    

#include <string>
#include <cstring>
#include <iostream>

#include "core/sparql/absyn.hpp"
#define YYDEBUG 1

using namespace wukong;

namespace wukong{
    SPARQLParser* parser = new SPARQLParser();
}
int yylex();
void yyerror(std::string error)
{
 std::cout<<error<<std::endl;
 exit(1);
}

%}
%error-verbose

/*Identifier is something like where or select*/

%union {
  int choice;
  char* str;
  std::vector<wukong::SPARQLParser::Filter*>* exp_list;
  std::vector<wukong::SPARQLParser::PatternGroup>* union_list;
  wukong::SPARQLParser::Element* element;
  wukong::SPARQLParser::Filter* filter;
  wukong::SPARQLParser::Pattern* pattern;
  wukong::SPARQLParser::PatternNode* pattern_node;
  wukong::SPARQLParser::PatternGroup* pattern_group;
}

%token none error eof 
  iri string_ variable identifier
  select_ ask_ where prefix union_ count distinct duplicates reduced /*predefined identifier*/
  colon semicolon comma dot underscore lcurly rcurly
  lparen rparen lbracket rbracket larrow rarrow
  anon equal not_equal less less_or_equal greater greater_or_equal
  at type not_ or_ and_ plus_ minus_ mul_ div_
  integer decimal double_ percent predicate time_interval
  optional_ filter order_by limit offset asc desc truee_ falsee_ bound from snapshot/* newly defined symbol */

%type<choice> PATTERN_SUFFIX ORDER_CHOICE RELATIONAL_CHOICE ADDITIVE_CHOICE MULTIPLICATIVE_CHOICE UNARY_CHOICE 
%type<str> identifier iri string_ variable time_interval
%type<str> integer decimal double_ /* will be converted to real number in the parser */
%type<element> PATTERN_ELEMENT
%type<filter> FILTER_NODE CONSTRAINT BUILTIN_CALL FUNC_CALL VALUE_LOGICAL 
%type<filter> EXP RELATIONAL_EXP NUMERIC_EXP BRACKETTED_EXP CONDITIONAL_AND_EXP CONDITIONAL_OR_EXP
%type<filter> ADD_EXP MUL_EXP UNARY_EXP PRIMARY_EXP NUMERIC
%type<filter> IRIREF_OR_FUNC RDF_LITERAL NUMERIC_LITERAL BOOL_LITERAL
%type<union_list> UNION_NODE
%type<exp_list> EXP_LIST ARG_LIST
%type<pattern> PATTERN
%type<pattern_group> PATTERN_GROUP PATTERN_LIST OPTIONAL_NODE
%type<pattern_node> PATTERN_NODE
%start SPARQLROOT 

%%
/*TODO blanknode*/
SPARQLROOT: TRAV {parser->postParsing();return 0;} 
;

TRAV: PREFIX_GROUP MAIN_CLAUSE
    | MAIN_CLAUSE
;

PREFIX_GROUP: PREFIX_NODE
            | PREFIX_NODE PREFIX_GROUP
;

PREFIX_NODE: prefix identifier colon iri {
    parser->addPrefix($2, $4);
}  
;

MAIN_CLAUSE: SELECT_WHERE
           | ASK_WHERE

SELECT_WHERE: select_ PROJECTION_MODIFIER PROJECTION_GROUP FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSE LIMIT_NODE OFFSET_NODE {
    parser->registerQueryType(SPARQLParser::Type_Select);
}
;

ASK_WHERE: ask_ FROM_CLAUSE WHERE_CLAUSE LIMIT_NODE OFFSET_NODE {
    parser->registerQueryType(SPARQLParser::Type_Ask);
}
;

PROJECTION_MODIFIER: identifier {parser->addProjectionModifier($1);}
                   | 
;

PROJECTION_GROUP: PROJECTION_NODE
                | PROJECTION_NODE PROJECTION_GROUP
                | mul_ {}

PROJECTION_NODE: variable {parser->addProjection($1);}
;

FROM_CLAUSE: from snapshot iri {parser->parseFromSnapshot($3);}
           | from time_interval {parser->parseFromTime($2);}
           |

WHERE_CLAUSE: where PATTERN_GROUP {
    parser->registerPatternGroup($2);
}

PATTERN_GROUP: lcurly PATTERN_LIST rcurly {$$ = $2;}
;

/*
 * 1.TripleBlock  e.g. (?B rdf:type ?A)+
 * 2.Optional e.g. Optional PatternGroup
 * 3.Filter e.g. Filter CONSTRAINT
 * 4.Union e.g. PatternGroup (union PatternGroup)*
 * 5.Group e.g. PatternGroup
 */

PATTERN_LIST: PATTERN_NODE {$$ = parser->makePatternGroup($1,NULL);}
            | PATTERN_NODE PATTERN_LIST {$$ = parser->makePatternGroup($1,$2);} 

PATTERN_NODE: PATTERN {$$ = parser->makePatternNode(SPARQLParser::Type_Pattern,$1,NULL,NULL,NULL);}
            | FILTER_NODE {$$ = parser->makePatternNode(SPARQLParser::Type_Filter,NULL,$1,NULL,NULL);}
            | OPTIONAL_NODE {$$ = parser->makePatternNode(SPARQLParser::Type_Optional,NULL,NULL,$1,NULL);}
            | UNION_NODE {$$ = parser->makePatternNode(SPARQLParser::Type_Union,NULL,NULL,NULL,$1);}

PATTERN: PATTERN_ELEMENT PATTERN_ELEMENT PATTERN_ELEMENT PATTERN_SUFFIX{
    $$ = parser->addPattern($1,$2,$3,$4,NULL);
}
       | time_interval PATTERN_ELEMENT PATTERN_ELEMENT PATTERN_ELEMENT PATTERN_SUFFIX{
    $$ = parser->addPattern($2,$3,$4,$5,$1);
}


PATTERN_SUFFIX: dot{$$=SPARQLParser::Suffix_Dot;}
              | larrow{$$=SPARQLParser::Suffix_LArrow;}
              | rarrow{$$=SPARQLParser::Suffix_RArrow;}
              |       {$$=SPARQLParser::Suffix_Blank;}

PATTERN_ELEMENT: variable {$$=parser->makeVariableElement($1);}
               | iri {$$=parser->makeIriElement($1,false);}
               | percent iri {$$=parser->makeIriElement($2,true);}
               | identifier colon identifier {$$=parser->makePrefixIriElement($1,$3,false);}
               | percent identifier colon identifier {$$=parser->makePrefixIriElement($2,$4,true);}
               | string_ {$$=parser->makeStringElement($1,NULL,NULL);}
               | string_ at identifier {$$=parser->makeStringElement($1,$3,NULL);}
               | string_ type iri {$$=parser->makeStringElement($1,NULL,$3);}
               | predicate {$$=parser->makePredicateElement();}
               | anon {$$=parser->makeAnonElement();}

OPTIONAL_NODE: optional_ PATTERN_GROUP OPTIONAL_NODE_SUFFIX {
    $$ = $2;
}

OPTIONAL_NODE_SUFFIX: dot {}
                    | {} 

UNION_NODE: PATTERN_GROUP union_ UNION_NODE {$$=parser->makePatternGroupList($1,$3);}
          | PATTERN_GROUP {$$=parser->makePatternGroupList($1,NULL);}

FILTER_NODE: filter CONSTRAINT {$$=$2;}

CONSTRAINT: BRACKETTED_EXP 
          | BUILTIN_CALL 
          | FUNC_CALL

BRACKETTED_EXP: lparen EXP rparen {$$=$2;}

BUILTIN_CALL: identifier ARG_LIST { $$ = parser->parseBuiltInCall($1,$2,NULL);}
            | bound lparen variable rparen {$$ = parser->parseBuiltInCall(const_cast<char*>("bound"),NULL,$3);}

EXP: CONDITIONAL_OR_EXP

CONDITIONAL_OR_EXP: CONDITIONAL_AND_EXP  {$$ = parser->parseDualExp($1,NULL,SPARQLParser::Filter::Or);}
                  | CONDITIONAL_AND_EXP or_ CONDITIONAL_OR_EXP {$$ = parser->parseDualExp($1,$3,SPARQLParser::Filter::Or);}

CONDITIONAL_AND_EXP: VALUE_LOGICAL {$$ = parser->parseDualExp($1,NULL,SPARQLParser::Filter::And);}
                   | VALUE_LOGICAL and_ CONDITIONAL_AND_EXP {$$ = parser->parseDualExp($1,$3,SPARQLParser::Filter::And);}

VALUE_LOGICAL: RELATIONAL_EXP 

RELATIONAL_EXP: NUMERIC_EXP {$$ = parser->parseDualExp($1,NULL,0);}
              | NUMERIC_EXP RELATIONAL_CHOICE NUMERIC_EXP {$$ = parser->parseDualExp($1,$3,$2);}

RELATIONAL_CHOICE: equal {$$=SPARQLParser::Filter::Equal;}
                 | not_equal {$$=SPARQLParser::Filter::NotEqual;}
                 | less {$$=SPARQLParser::Filter::Less;}
                 | less_or_equal {$$=SPARQLParser::Filter::LessOrEqual;}
                 | greater {$$=SPARQLParser::Filter::Greater;}
                 | greater_or_equal {$$=SPARQLParser::Filter::GreaterOrEqual;}

NUMERIC_EXP: ADD_EXP

ADD_EXP: MUL_EXP {$$ = parser->parseDualExp($1,NULL,0);}
       | MUL_EXP ADDITIVE_CHOICE ADD_EXP {$$ = parser->parseDualExp($1,$3,$2);}

ADDITIVE_CHOICE: plus_ {$$=SPARQLParser::Filter::Plus;}
               | minus_ {$$=SPARQLParser::Filter::Minus;}

MUL_EXP: UNARY_EXP {$$ = parser->parseDualExp($1,NULL,0);}
       | UNARY_EXP MULTIPLICATIVE_CHOICE MUL_EXP {$$ = parser->parseDualExp($1,$3,$2);}

MULTIPLICATIVE_CHOICE: mul_ {$$=SPARQLParser::Filter::Mul;}
                     | div_ {$$=SPARQLParser::Filter::Div;}

UNARY_EXP: UNARY_CHOICE PRIMARY_EXP {$$ = parser->parseUnaryExp($2,$1);}
         | PRIMARY_EXP {$$ = $1;}

UNARY_CHOICE : not_ {$$=SPARQLParser::Filter::Not;}
             | plus_ {$$=SPARQLParser::Filter::UnaryPlus;}
             | minus_ {$$=SPARQLParser::Filter::UnaryMinus;}

PRIMARY_EXP: BRACKETTED_EXP 
           | BUILTIN_CALL
           | IRIREF_OR_FUNC
           | RDF_LITERAL
           | NUMERIC_LITERAL
           | BOOL_LITERAL
           | variable {$$ = parser->parseVariable($1);}

FUNC_CALL: iri ARG_LIST {$$ = parser->parseIRIOrFunc($1,$2);}

IRIREF_OR_FUNC: iri {$$ = parser->parseIRIOrFunc($1,NULL);}
              | FUNC_CALL

ARG_LIST: lparen rparen {$$ = new std::vector<SPARQLParser::Filter*>();}
       |  lparen EXP_LIST rparen {$$ = $2;}

EXP_LIST: EXP {$$ = parser->makeExpList($1,NULL);}
        | EXP comma EXP_LIST {$$ = parser->makeExpList($1,$3);}

RDF_LITERAL: string_ { $$ = parser->parseRDFLiteral($1,NULL,NULL);}
           | string_ at identifier{ $$ = parser->parseRDFLiteral($1,$3,NULL);}
           | string_ type iri { $$ = parser->parseRDFLiteral($1,NULL,$3);}

NUMERIC_LITERAL: NUMERIC 
               | plus_ NUMERIC {$$ = $2;} 

NUMERIC: integer {$$ = parser->parseNumericLiteral($1,SPARQLParser::Numeric_Integer);}
       | decimal {$$ = parser->parseNumericLiteral($1,SPARQLParser::Numeric_Decimal);}
       | double_ {$$ = parser->parseNumericLiteral($1,SPARQLParser::Numeric_Double);}
    
BOOL_LITERAL: truee_ {$$ = parser->parseBoolLiteral(true);}
            | falsee_ {$$ = parser->parseBoolLiteral(false);}


/*ORDER BY ASC(?X) DESC(?Y1)*/
ORDER_CLAUSE: order_by ORDER_LIST
            | 

ORDER_LIST: ORDER_NODE 
          | ORDER_NODE ORDER_LIST 

ORDER_NODE: ORDER_CHOICE lparen count rparen {
                parser->addOrder(NULL,$1);
            } /* order by asc(count) */
          | ORDER_CHOICE lparen variable rparen {parser->addOrder($3,$1);} /* order by ?x */
          | variable {parser->addOrder($1);} /*order by ?x*/
          | count {parser->addOrder(NULL);} /*order by count*/

ORDER_CHOICE: asc{$$ = SPARQLParser::Order_Asc;}
            | desc{$$ = SPARQLParser::Order_Desc;}

LIMIT_NODE: limit integer {parser->addLimit(atoi($2));}
          | 

OFFSET_NODE: offset integer {parser->addOffset(atoi($2));}
           | 
%%