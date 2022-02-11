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

extern int yyparse(void);
extern FILE *yyin;

namespace wukong {
class HyperParser;
extern HyperParser* parser;

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
    // place holder of pattern type (a special group of objects)
    const static ssid_t PTYPE_PH = std::numeric_limits<ssid_t>::min() + 1;
    const static ssid_t DUMMY_ID = std::numeric_limits<ssid_t>::min();
    const static ssid_t PREDICATE_ID = 0;

    // str2id mapping for pattern constants
    // (e.g., <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> 1)
    StringServer *str_server;

    HyperQuery::PatternType transfer_type(const HyperParser::PatternType src) {
        // TODO: finish the conversion(using char ?)
        return static_cast<HyperQuery::PatternType>(src);
    }

    /// HyperParser::Element to ssid
    ssid_t transfer_element(const HyperParser::Element &e) {
        switch (e.type) {
        case HyperParser::Element::Variable:
            return e.id;
        case HyperParser::Element::IRI:
        {
            std::string str = "<" + e.value + ">"; // IRI
            if (!str_server->exist(str)) {
                if (!str_server->exist_he(str)) {
                    logstream(LOG_ERROR) << "Unknown IRI: " + str << LOG_endl;
                    throw WukongException(SYNTAX_ERROR);
                } else {
                    return str_server->str2id_he(str);
                }
            } else {
                return str_server->str2id(str);
            }
        }
        default:
            throw WukongException(SYNTAX_ERROR);
        }

        throw WukongException(SYNTAX_ERROR);
    }

    std::vector<ssid_t> transfer_element_list(const HyperParser::ElementList &el) {
        std::vector<ssid_t> inputs;
        for (auto &&e : el) inputs.insert(inputs.begin(), transfer_element(e));
        return inputs;
    }

    /// HyperParser::PatternGroup to SPARQLQuery::PatternGroup
    void transfer_pg(HyperParser::PatternGroup &src, HyperQuery::PatternGroup &dst) {
        // Patterns
        for (auto const &p : src.patterns) {
            // parse all the elements
            HyperQuery::PatternType type = transfer_type(p.type);
            std::vector<ssid_t> input_vars = transfer_element_list(p.input_vars);
            ssid_t output_var = transfer_element(p.output_var);
            sid_t bind_node = transfer_element(p.bind_node);
            uint32_t k = (uint32_t)p.k;
            // check invalid elements
            ASSERT_GT(input_vars.size(), 0);
            ASSERT_LT(output_var, 0);
            ASSERT_GE(p.k, 0);
            // construct and add the new pattern
            HyperQuery::Pattern pattern(type, input_vars, output_var, bind_node, k);
            dst.patterns.push_back(pattern);
        }

    }

    void transfer(const HyperParser &sp, HyperQuery &sq) {
        // required varaibles of SELECT clause
        for (HyperParser::projection_iterator iter = sp.projectionBegin();
                iter != sp.projectionEnd();
                iter ++)
            sq.result.required_vars.push_back(*iter);

        // pattern group (patterns, union, filter, optional)
        HyperParser::PatternGroup group = sp.getPatterns();
        transfer_pg(group, sq.pattern_group);

        sq.result.nvars = sp.getVariableCount();
    }

public:
    // the stat of query parsing
    std::string strerror;

    Parser(StringServer *_ss): str_server(_ss) { }

    /// a single query
    int parse(std::string fname, HyperQuery &sq) {
        yyin = fopen(fname.c_str(),"r");
        if (!yyin) {
            return FILE_NOT_FOUND; // file not found
        }
        parser->clear();
        try {
            yyparse();
            transfer(*parser, sq);
        } catch (const HyperParser::ParserException &e) {
            logstream(LOG_ERROR) << "Failed to parse a SPARQL query: "
                                 << e.message << LOG_endl;
            fclose(yyin);
            return SYNTAX_ERROR;
        }

        logstream(LOG_INFO) << "Parsing a SPARQL query is done." << LOG_endl;
        return SUCCESS;
    }

    int parse(std::string fname, SPARQLQuery &sq) {return 0;}
    int parse_template(std::string fname, SPARQLQuery_Template &sqt) {return 0;}
};

} // namespace wukong