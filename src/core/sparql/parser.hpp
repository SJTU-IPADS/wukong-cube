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

#include "core/sparql/query.hpp"
#include "core/sparql/absyn.hpp"

#include "stringserver/string_mapping.hpp"

// utils
#include "utils/assertion.hpp"

extern struct yy_buffer_state* yy_scan_string(const char* str);
extern void yy_delete_buffer(struct yy_buffer_state* buffer);
extern int yyparse(void);
extern void yyrestart(FILE * input_file);
extern FILE *yyin;

namespace wukong {
class SPARQLParser;
extern SPARQLParser* parser;

// Read a stream into a string
static std::string read_input(std::istream &in) {
    std::string result;
    while (true) {
        std::string s;
        std::getline(in, s);
        result += s;
        result += '\n';
        if (!in.good())
            break;
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
    StringMapping *str_mapping;

    int tid;

    /// SPARQLParser::Element to ssid
    ssid_t transfer_element(const SPARQLParser::Element &e) {
        switch (e.type) {
        case SPARQLParser::Element::Variable:
            return e.id;
        case SPARQLParser::Element::Literal:
        {
            std::string str = "";
            // string with language tag
            //      Ex. "SuperPatriot"@en
            //      value stored in string server: "SuperPatriot"@en
            //      e.value: SuperPatriot , e.subTypeValue: en
            if (e.subType == SPARQLParser::Element::CustomLanguage)
                str = "\"" + e.value + "\"" + "@" + e.subTypeValue;
            // normal case
            else
                str = "\"" + e.value + "\"";

            auto map_result = str_mapping->str2id(tid, str);
            if (!map_result.first) {
                logstream(LOG_ERROR) << "Unknown Literal: " + str << LOG_endl;
                throw WukongException(SYNTAX_ERROR);
            }
            return map_result.second;
        }
        case SPARQLParser::Element::IRI:
        {
            std::string str = "<" + e.value + ">"; // IRI
            auto map_result = str_mapping->str2id(tid, str);
            if (!map_result.first) {
                logstream(LOG_ERROR) << "Unknown IRI: " + str << LOG_endl;
                throw WukongException(SYNTAX_ERROR);
            }
            return map_result.second;
        }
        case SPARQLParser::Element::Template:
            return PTYPE_PH;
        case SPARQLParser::Element::Predicate:
            return PREDICATE_ID;
        default:
            throw WukongException(SYNTAX_ERROR);
        }

        throw WukongException(SYNTAX_ERROR);
    }

    /// SPARQLParser::Filter to SPARQLQuery::Filter
    void transfer_filter(SPARQLParser::Filter &src, SPARQLQuery::Filter &dst) {
        dst.type = (SPARQLQuery::Filter::Type)src.type;
        dst.value = src.value;
        dst.valueArg = src.valueArg;
        if (src.arg1 != NULL) {
            dst.arg1 = new SPARQLQuery::Filter();
            transfer_filter(*src.arg1, *dst.arg1);
        }
        if (src.arg2 != NULL) {
            dst.arg2 = new SPARQLQuery::Filter();
            transfer_filter(*src.arg2, *dst.arg2);
        }
        if (src.arg3 != NULL) {
            dst.arg3 = new SPARQLQuery::Filter();
            transfer_filter(*src.arg3, *dst.arg3);
        }
    }

    void transfer_interval_type(SPARQLQuery::TimeIntervalPattern& pat, SPARQLParser::Element::Type ts_type, SPARQLParser::Element::Type te_type) {
        switch (const_pair(ts_type, te_type)) {
        case const_pair(SPARQLParser::Element::TimeStamp, SPARQLParser::Element::TimeStamp):
            pat.type = SPARQLQuery::TimeIntervalType::VALUE_VALUE;
            break;
        case const_pair(SPARQLParser::Element::TimeStamp, SPARQLParser::Element::Variable):
            pat.type = SPARQLQuery::TimeIntervalType::VALUE_VAR;
            break;
        case const_pair(SPARQLParser::Element::Variable, SPARQLParser::Element::TimeStamp):
            pat.type = SPARQLQuery::TimeIntervalType::VAR_VALUE;
            break;
        case const_pair(SPARQLParser::Element::Variable, SPARQLParser::Element::Variable):
            pat.type = SPARQLQuery::TimeIntervalType::VAR_VAR;
            break;
        case const_pair(SPARQLParser::Element::Invalid, SPARQLParser::Element::Invalid):
            pat.type = SPARQLQuery::TimeIntervalType::UNDEFINED;
            break;
        default:
            ASSERT_ERROR_CODE(false, UNKNOWN_PATTERN);
        }
    }

    /// SPARQLParser::PatternGroup to SPARQLQuery::PatternGroup
    void transfer_pg(SPARQLParser::PatternGroup &src, SPARQLQuery::PatternGroup &dst) {
        // Patterns
        for (auto const &p : src.patterns) {
            ssid_t subject = transfer_element(p.subject);
            ssid_t predicate = transfer_element(p.predicate);
            dir_t direction = (dir_t)p.direction;
            ssid_t object = transfer_element(p.object);

            SPARQLQuery::Pattern pattern(subject, predicate, direction, object);

            pattern.time_interval = SPARQLQuery::TimeIntervalPattern(p.ts.timestamp, p.te.timestamp, p.ts.id, p.te.id);
            transfer_interval_type(pattern.time_interval, p.ts.type, p.te.type);

            pattern.pred_type = str_mapping->pid2type[predicate];
            if ((pattern.pred_type != (char)SID_t) && !Global::enable_vattr) {
                logstream(LOG_ERROR) << "Must enable attribute support"
                                     << LOG_endl;
                ASSERT(false);
            }

            dst.patterns.push_back(pattern);
        }

        // Filters
        for (auto &f : src.filters) {
            dst.filters.push_back(SPARQLQuery::Filter());
            transfer_filter(f, dst.filters.back());
        }

        // Unions
        for (auto &u : src.unions) {
            dst.unions.push_back(SPARQLQuery::PatternGroup());
            transfer_pg(u, dst.unions.back());
        }

        // Optional
        for (auto &o : src.optional) {
            dst.optional.push_back(SPARQLQuery::PatternGroup());
            transfer_pg(o, dst.optional.back());
        }

        /// TODO: support other Grammars in PatternGroup
    }

    void transfer(const SPARQLParser &sp, SPARQLQuery &sq) {
        sq.ts = sp.ts;
        sq.te = sp.te;

        // query type
        sq.q_type = static_cast<SPARQLQuery::QueryType>(sp.getQueryType());

        // required varaibles of SELECT clause
        for (SPARQLParser::projection_iterator iter = sp.projectionBegin();
                iter != sp.projectionEnd();
                iter ++) {
            sq.result.required_vars.push_back(*iter);
            sq.result.required_vars_name.push_back(sp.getVariableName(*iter));
        }

        // pattern group (patterns, union, filter, optional)
        SPARQLParser::PatternGroup group = sp.getPatterns();
        transfer_pg(group, sq.pattern_group);

        sq.result.nvars = sp.getVariableCount();

        // orders
        if (sq.q_type == SPARQLQuery::SELECT)
            for (SPARQLParser::order_iterator iter = sp.orderBegin();
                    iter != sp.orderEnd();
                    iter ++)
                sq.orders.push_back(SPARQLQuery::Order((*iter).id, (*iter).descending));

        // limit and offset
        sq.limit = sp.getLimit();
        sq.offset = sp.getOffset();

        // distinct
        if (sq.q_type == SPARQLQuery::SELECT)
            if ((sp.getProjectionModifier() == SPARQLParser::ProjectionModifier::Modifier_Distinct)
                    || (sp.getProjectionModifier() == SPARQLParser::ProjectionModifier::Modifier_Reduced))
                sq.distinct = true;

        // corun optimization (disabled)
        if (sq.corun_enabled = sp.isCorunEnabled()) {
            sq.corun_step = sp.getCorunStep();
            sq.fetch_step = sp.getFetchStep();

            if (!Global::use_rdma) {
                // TODO: corun optimization is not supported w/o RDMA
                logstream(LOG_WARNING) << "RDMA is not enabled, skip corun optimization!" << LOG_endl;
                sq.corun_enabled = false; // skip
            }
        }
    }

    void transfer_template(const SPARQLParser &sp, SPARQLQuery_Template &sqt) {
        // required varaibles of SELECT clause
        for (SPARQLParser::projection_iterator iter = sp.projectionBegin();
                iter != sp.projectionEnd();
                iter ++)
            sqt.required_vars.push_back(*iter);

        // pattern group (patterns)
        // FIXME: union, filter, optional (unsupported now)
        SPARQLParser::PatternGroup group = sp.getPatterns();
        int pos = 0;
        for (auto &p : group.patterns) {
            ssid_t subject = transfer_element(p.subject);
            ssid_t predicate = transfer_element(p.predicate);
            dir_t direction = (dir_t)p.direction;
            ssid_t object = transfer_element(p.object);

            SPARQLQuery::Pattern pattern(subject, predicate, direction, object);

            pattern.time_interval = SPARQLQuery::TimeIntervalPattern(p.ts.timestamp, p.te.timestamp, p.ts.id, p.te.id);
            transfer_interval_type(pattern.time_interval, p.ts.type, p.te.type);
            
            // template pattern
            if (subject == PTYPE_PH) {
                sqt.ptypes_str.push_back("<" + p.subject.value + ">"); // IRI
                sqt.ptypes_pos.push_back(pos + 0); // subject
            }

            if (object == PTYPE_PH) {
                sqt.ptypes_str.push_back("<" + p.object.value + ">"); // IRI
                sqt.ptypes_pos.push_back(pos + 3); // object
            }

            pattern.pred_type = (char)str_mapping->pid2type[predicate];
            if ((pattern.pred_type != (char)SID_t) && !Global::enable_vattr) {
                logstream(LOG_ERROR) << "Must enable attribute support" << LOG_endl;
                ASSERT(false);
            }

            sqt.pattern_group.patterns.push_back(pattern);
            pos += 4;
        }

        sqt.nvars = sp.getVariableCount();

        // FIXME: orders (unsupported now)

        // FIXME: limit and offset (unsupported now)

        // FIXME: distinct (unsupported now)

        // FIXME: corun optimization (unsupported now)
    }

public:
    // the stat of query parsing
    std::string strerror;

    Parser(int tid, StringMapping *_ss): tid(tid), str_mapping(_ss) { }

    /// a single query
    int parse(std::istream &in, SPARQLQuery &sq) {
        parser->clear();
        try {
            std::string query_text = read_input(in);
            char* str = new char[query_text.length() + 1];
            std::strcpy(str, query_text.c_str());
            struct yy_buffer_state* bufferState = yy_scan_string(str);
            yyparse();
            yy_delete_buffer(bufferState);
            delete[] str;
            transfer(*parser, sq);
        } catch (const SPARQLParser::ParserException &e) {
            logstream(LOG_ERROR) << "Failed to parse a SPARQL query: "
                                 << e.message << LOG_endl;
            return SYNTAX_ERROR;
        }

        // check if using custom grammar when planner is on
        if (parser->isUsingCustomGrammar() && Global::enable_planner) {
            logstream(LOG_ERROR) << "Unsupported custom grammar in SPARQL planner!"
                                 << LOG_endl;
            return SYNTAX_ERROR;
        }

        logstream(LOG_INFO) << "Parsing a SPARQL query is done." << LOG_endl;
        return SUCCESS;
    }

    /// a class of queries
    int parse_template(std::istream &in, SPARQLQuery_Template &sqt) {
        parser->clear();
        try {
            std::string query_text = read_input(in);
            char* str = new char[query_text.length() + 1];
            std::strcpy(str, query_text.c_str());
            struct yy_buffer_state* bufferState = yy_scan_string(str);
            yyparse();
            yy_delete_buffer(bufferState);
            delete[] str;
            transfer_template(*parser, sqt);
        } catch (const SPARQLParser::ParserException &e) {
            logstream(LOG_ERROR) << "Failed to parse a SPARQL template: "
                                 << e.message << LOG_endl;
            return SYNTAX_ERROR;
        }

        logstream(LOG_INFO) << "Parsing a SPARQL template is done." << LOG_endl;
        return SUCCESS;
    }
};

} // namespace wukong