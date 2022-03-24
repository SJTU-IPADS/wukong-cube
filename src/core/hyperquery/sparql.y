%{
#include <stdio.h>
#include <stdlib.h>    

#include <string>
#include <cstring>
#include <iostream>

#include "core/hyperquery/absyn.hpp"
#define YYDEBUG 1

using namespace wukong;

namespace wukong {
    HyperParser* parser = new HyperParser();
}

int yylex();
void yyerror(std::string error);

%}
%error-verbose

/*Identifier is something like where or select*/

%union {
  int number;
  char* str;
  wukong::HyperParser::Param* param;
  wukong::HyperParser::ParamList* param_list;
  wukong::HyperParser::Element* element;
  wukong::HyperParser::ElementList* element_list;
  wukong::HyperParser::Pattern* pattern;
  wukong::HyperParser::PatternGroup* pattern_group;
}

%token none error eof 
  iri string_ variable identifier
  select_ where prefix  distinct duplicates reduced /*predefined identifier*/
  colon semicolon comma dot underscore lcurly rcurly
  lparen rparen lbracket rbracket larrow rarrow
  anon equal not_equal less less_or_equal greater greater_or_equal
  at type not_ or_ and_ plus_ minus_ mul_ div_
  integer decimal double_ percent
  buildin etype vtype edges vertices intersect_edges  /* hyper query symbol */
  contain_edges in_edges intersect_vertices


%type<number> PATTERN_TYPE PATTERN_PARAM_TYPE
%type<str> identifier iri variable integer string_
%type<param> PATTERN_PARAM
%type<param_list> PATTERN_PARAM_LIST
%type<element> PATTERN_ELEMENT
%type<element_list> PATTERN_ELEMENT_LIST PATTERN_ELEMENT_GROUP
%type<pattern> PATTERN
%type<pattern_group> PATTERN_GROUP PATTERN_LIST
%start HYPERROOT 

%%
/*TODO blanknode*/
HYPERROOT: TRAV {parser->postParsing();return 0;} 
;

TRAV: PREFIX_GROUP SELECT_WHERE
    | SELECT_WHERE
;

PREFIX_GROUP: PREFIX_NODE
            | PREFIX_NODE PREFIX_GROUP
;

PREFIX_NODE: prefix identifier colon iri {
    parser->addPrefix($2, $4);
}
;

SELECT_WHERE: select_ PROJECTION_GROUP WHERE_CLAUSE
;

PROJECTION_GROUP: PROJECTION_NODE
                | PROJECTION_NODE PROJECTION_GROUP
                | mul_ {}

PROJECTION_NODE: variable {parser->addProjection($1);}
;

WHERE_CLAUSE: where PATTERN_GROUP {
    parser->registerPatternGroup($2);
}

PATTERN_GROUP: lcurly PATTERN_LIST rcurly {$$ = $2;}
;

PATTERN_LIST: PATTERN {$$ = parser->makePatternGroup($1,NULL);}
            | PATTERN PATTERN_LIST {$$ = parser->makePatternGroup($1,$2);} 

PATTERN: PATTERN_ELEMENT_GROUP PATTERN_META PATTERN_ELEMENT {
    $$ = parser->addPattern($1,$3,0);
}

PATTERN_ELEMENT_GROUP: PATTERN_ELEMENT {$$=parser->makeElementList($1, NULL);} 
                    | lbracket PATTERN_ELEMENT_LIST rbracket {$$ = $2;}

PATTERN_ELEMENT_LIST: PATTERN_ELEMENT {$$=parser->makeElementList($1, NULL);}
                    | PATTERN_ELEMENT comma PATTERN_ELEMENT_LIST {$$=parser->makeElementList($1, $3);}

PATTERN_META: buildin colon PATTERN_TYPE {parser->addPatternMeta($3, NULL);}
            | buildin colon PATTERN_TYPE lparen PATTERN_PARAM_LIST rparen {parser->addPatternMeta($3, $5);}

PATTERN_PARAM_LIST: PATTERN_PARAM {$$=parser->makeParamList($1, NULL);}
                    | PATTERN_PARAM comma PATTERN_PARAM_LIST {$$=parser->makeParamList($1, $3);}

PATTERN_PARAM: PATTERN_ELEMENT {$$=parser->makeParam(HyperParser::ParamType::NO_TYPE, $1);}
             | PATTERN_PARAM_TYPE PATTERN_ELEMENT {$$=parser->makeParam($1, $2);} 

PATTERN_PARAM_TYPE: etype {$$=HyperParser::ParamType::P_ETYPE;}
                  | vtype {$$=HyperParser::ParamType::P_VTYPE;}
                  | equal {$$=HyperParser::ParamType::P_EQ;}
                  | not_equal {$$=HyperParser::ParamType::P_NE;}
                  | less {$$=HyperParser::ParamType::P_LT;}
                  | less_or_equal {$$=HyperParser::ParamType::P_LE;}
                  | greater {$$=HyperParser::ParamType::P_GT;}
                  | greater_or_equal {$$=HyperParser::ParamType::P_GE;}

PATTERN_ELEMENT: iri {$$=parser->makeIriElement($1, false);}
                | identifier colon identifier {$$=parser->makePrefixIriElement($1,$3,false);}
                | string_ {$$=parser->makeLiteralElement($1, false);}
                | variable {$$=parser->makeVariableElement($1);}
                | integer {$$=parser->makeIntElement(atoi($1));}
                | percent iri {$$=parser->makeIriElement($2, true);}
                | percent identifier colon identifier {$$=parser->makePrefixIriElement($2,$4,true);}
                | percent string_ {$$=parser->makeLiteralElement($2, true);}

PATTERN_TYPE: etype {$$=HyperParser::PatternType::GE;}
            | vtype {$$=HyperParser::PatternType::GV;}
            | edges {$$=HyperParser::PatternType::V2E;}
            | vertices {$$=HyperParser::PatternType::E2V;}
            | intersect_edges {$$=HyperParser::PatternType::E2E_ITSCT;}
            | contain_edges {$$=HyperParser::PatternType::E2E_CT;}
            | in_edges {$$=HyperParser::PatternType::E2E_IN;}
            | intersect_vertices {$$=HyperParser::PatternType::V2V;}

%%