/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <cstdlib>

#include <boost/unordered_map.hpp>
#include <boost/algorithm/string.hpp>

#include "utils/time_tool.hpp"
#include "core/common/type.hpp"
#include "core/common/string_server.hpp"

#include "core/sparql/query.hpp"
#include "core/hyperquery/query.hpp"
#include "core/hyperquery/absyn.hpp"

// utils
#include "utils/assertion.hpp"

// lexer && parser
#include "sparql.tab.h"
#include "lex.yy.h"

extern int yyparse(yyscan_t scanner, wukong::HyperParser* parser);
extern int yylineno;

namespace wukong {

// Read a stream into a string
static std::string read_input(std::istream &in) {
    std::string result;
    while (true) {
        std::string s;
        std::getline(in, s);
        result += s;
        if (!in.good())
            break;
        result += '\n';
    }

    return result;
}


/**
 * Q := SELECT RD WHERE GP
 *
 * The types of tokens (supported)
 * 0. SPARQL's Prefix e.g., PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * 1. SPARQL's Keyword (incl. SELECT, WHERE)
 *
 * 2. pattern's constant e.g., <http://www.Department0.University0.edu>
 * 3. pattern's variable e.g., ?X
 * 4. pattern's random-constant e.g., %ub:GraduateCourse (extended by Wukong in batch-mode)
 *
 */
class Parser {
private:
    HyperParser* parser;
    
    // place holder of pattern type (a special group of objects)
    const static ssid_t PTYPE_PH = std::numeric_limits<ssid_t>::min() + 1;
    const static ssid_t DUMMY_ID = std::numeric_limits<ssid_t>::min();
    const static ssid_t PREDICATE_ID = 0;

    // template
    bool parse_tpl = false;
    std::vector<sid_t> tpls_types; // the Types of random-constants
    std::vector<std::pair<int, HyperQuery_Template::PatternPos>> tpls_pos; // the locations of random-constants

    // str2id mapping for pattern constants
    // (e.g., <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> 1)
    StringServer *str_server;

    /// HyperParser::Element to ssid
    /// output is either a variable or a htid/tid
    ssid_t transfer_output(const HyperParser::Element &e, int pattern_index) {
        // check validation
        if (e.tplt && !parse_tpl) {
            logstream(LOG_ERROR) << "UnExpected Template in Pattern Output." << LOG_endl;
            throw WukongException(SYNTAX_ERROR);   
        }

        // transfer element
        std::string str = e.value;
        switch (e.type) {
        case HyperParser::Element::Variable:
            return e.id;
        case HyperParser::Element::IRI:
            str = "<" + str + ">"; // IRI
        case HyperParser::Element::Literal:
            if (!str_server->exist(str)) {
                if (!str_server->exist_he(str)) {
                    logstream(LOG_ERROR) << "Unknown IRI: " + str << LOG_endl;
                    throw WukongException(SYNTAX_ERROR);
                } else {
                    logstream(LOG_ERROR) << "UnExpected HyperEdge in Parameter: " + str << LOG_endl;
                    throw WukongException(SYNTAX_ERROR);
                    // return HyperQuery::Param(HEID_t, str_server->str2id_he(str));
                }
            } else {
                sid_t id = str_server->str2id(str);
                if (e.tplt && parse_tpl) {
                    // register template info
                    tpls_types.push_back(id);
                    tpls_pos.push_back(std::make_pair(pattern_index, HyperQuery_Template::PT_OUTPUT));
                }
                return id;
            }
            break;
        default:
            throw WukongException(SYNTAX_ERROR);
        }

        throw WukongException(SYNTAX_ERROR);
    }

    /// HyperParser::ElementList to input lists in HyperQuery::Pattern
    void transfer_input_list(const HyperParser::ElementList &el, HyperQuery::Pattern &pt, int pattern_index) {
        for (auto &&e : el) {
            // check validation
            if (e.tplt && !parse_tpl) {
                logstream(LOG_ERROR) << "UnExpected Template in Pattern Output." << LOG_endl;
                throw WukongException(SYNTAX_ERROR);   
            }

            // transfer element
            std::string str = e.value;
            switch (e.type) {
            case HyperParser::Element::Variable:
                pt.input_vars.push_back(e.id);
                break;
            case HyperParser::Element::IRI:
                str = "<" + str + ">"; // IRI
            case HyperParser::Element::Literal:
                if (!str_server->exist(str)) {
                    if (!str_server->exist_he(str)) {
                        logstream(LOG_ERROR) << "Unknown IRI: " + str << LOG_endl;
                        throw WukongException(SYNTAX_ERROR);
                    } else {
                        pt.input_eids.push_back(str_server->str2id_he(str));
                    }
                } else {
                    sid_t id = str_server->str2id(str);
                    if (e.tplt && parse_tpl) {
                        // register template info
                        tpls_types.push_back(id);
                        tpls_pos.push_back(std::make_pair(pattern_index, HyperQuery_Template::PT_INPUT));
                    } 
                    else pt.input_vids.push_back(id);
                }
                break;
            default:
                e.print_element();
                throw WukongException(SYNTAX_ERROR);
            }
        }
    }

    /// HyperParser::ElementList to HyperQuery::Param list
    /// the default type of a Int parameter is HyperQuery::P_ETYPE
    /// the default type of a SID_t parameter is HyperQuery::P_GE
    void transfer_param_list(const HyperParser::ParamList &el, HyperQuery::Pattern &pt) {
        for (auto &&e : el) {
            HyperQuery::ParamType type = static_cast<HyperQuery::ParamType>(e.type);
            std::string str = e.value.value; // IRI
            // tranfer parameter value
            switch (e.value.type) {
            case HyperParser::Element::IRI:
                str = "<" + str + ">"; // IRI
            case HyperParser::Element::Literal:
                if (!str_server->exist(str)) {
                    if (!str_server->exist_he(str)) {
                        logstream(LOG_ERROR) << "Unknown IRI: " + str << LOG_endl;
                        throw WukongException(SYNTAX_ERROR);
                    } else {
                        logstream(LOG_ERROR) << "UnExpected HyperEdge in Parameter: " + str << LOG_endl;
                        throw WukongException(SYNTAX_ERROR);
                        // return HyperQuery::Param(HEID_t, str_server->str2id_he(str));
                    }
                } else {
                    if (type == HyperQuery::NO_TYPE) type = HyperQuery::P_ETYPE;
                    pt.params.insert(pt.params.begin(), HyperQuery::Param(type, SID_t, str_server->str2id(str)));
                }
                break;
            case HyperParser::Element::Int:
                ASSERT_GE(e.value.num, 0);     // int param should be a positive number
                if (type == HyperQuery::NO_TYPE) type = HyperQuery::P_GE;
                pt.params.insert(pt.params.begin(), HyperQuery::Param(type, INT_t, e.value.num));
                break;
            default:
                logstream(LOG_ERROR) << "UnRecognized Parameter" << LOG_endl;
                throw WukongException(SYNTAX_ERROR);
            }
        }
    }

    /// HyperParser::PatternGroup to HyperQuery::PatternGroup
    void transfer_pg(HyperParser::PatternGroup &src, HyperQuery::PatternGroup &dst) {
        int step = 0;
        // Patterns
        for (auto const &p : src.patterns) {
            HyperQuery::Pattern pattern;
            // parse all the elements
            pattern.type = static_cast<HyperQuery::PatternType>(p.type);
            pattern.output_var = transfer_output(p.output_var, step);
            transfer_input_list(p.input_vars, pattern, step);
            transfer_param_list(p.params, pattern);
            // check invalid elements
            ASSERT_GT(p.input_vars.size(), 0);
            // add the new pattern
            dst.patterns.push_back(pattern);
            step++;
        }
    }

    void transfer(const HyperParser &sp, HyperQuery &hq) {
        // required varaibles of SELECT clause
        for (HyperParser::projection_iterator iter = sp.projectionBegin();
                iter != sp.projectionEnd();
                iter ++)
            hq.result.required_vars.push_back(*iter);

        // pattern group (patterns, union, filter, optional)
        HyperParser::PatternGroup group = sp.getPatterns();
        transfer_pg(group, hq.pattern_group);

        hq.result.nvars = sp.getVariableCount();
    }

    void transfer_template(const HyperParser &sp, HyperQuery_Template &hqt) {
        parse_tpl = true;

        // required varaibles of SELECT clause
        for (HyperParser::projection_iterator iter = sp.projectionBegin();
                iter != sp.projectionEnd();
                iter ++)
            hqt.required_vars.push_back(*iter);
        hqt.nvars = sp.getVariableCount();

        // pattern group (patterns, union, filter, optional)
        HyperParser::PatternGroup group = sp.getPatterns();
        transfer_pg(group, hqt.pattern_group);
        
        // load template info
        hqt.tpls_id.swap(tpls_types);
        hqt.tpls_pos.swap(tpls_pos);
        parse_tpl = false;
    }

public:
    // the stat of query parsing
    std::string strerror;

    Parser(StringServer *_ss): str_server(_ss) {
        parser = new HyperParser();
    }

    /// a single query
    int parse(std::string fname, HyperQuery &hq) {
        FILE *in = fopen(fname.c_str(),"r");
        if (!in) return FILE_NOT_FOUND; // file not found

        // define and init scanner
        yyscan_t scanner;
        if (yylex_init(&scanner)) {
            logstream(LOG_ERROR) << "Failed to init a HYPER scanner: " << LOG_endl;
            return SYNTAX_ERROR;
        }
        yyrestart(in, scanner);

        try {
            parser->clear();
            yyparse(scanner, parser);
            transfer(*parser, hq);
        } catch (const HyperParser::ParserException &e) {
            logstream(LOG_ERROR) << "Failed to parse a HYPER query: "
                                 << e.message << LOG_endl;
            return SYNTAX_ERROR;
        }
        fclose(in);
        yylex_destroy(scanner);

        logstream(LOG_INFO) << "Parsing a HYPER query is done." << LOG_endl;
        return SUCCESS;
    }

    /// a query template
    int parse_template(std::string fname, HyperQuery_Template &hqt) {
        // open file
        FILE *in = fopen(fname.c_str(),"r");
        if (!in) return FILE_NOT_FOUND; // file not found

        // define and init scanner
        yyscan_t scanner;
        if (yylex_init(&scanner)) {
            logstream(LOG_ERROR) << "Failed to init a HYPER scanner: " << LOG_endl;
            return SYNTAX_ERROR;
        }
        yyrestart(in, scanner);

        try {
            parser->clear();
            yyparse(scanner, parser);
            transfer_template(*parser, hqt);
        } catch (const HyperParser::ParserException &e) {
            logstream(LOG_ERROR) << "Failed to parse a HYPER query: "
                                 << e.message << LOG_endl;
            return SYNTAX_ERROR;
        }
        fclose(in);
        yylex_destroy(scanner);

        logstream(LOG_INFO) << "Parsing a HYPER query template is done." << LOG_endl;
        return SUCCESS;
    }

    int parse(std::string fname, SPARQLQuery &sq) {return 0;}
    int parse_template(std::string fname, SPARQLQuery_Template &sqt) {return 0;}
};

} // namespace wukong