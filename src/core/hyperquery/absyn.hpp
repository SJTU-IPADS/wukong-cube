#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <cstdlib>

#include "utils/time_tool.hpp"
#include <boost/unordered_map.hpp>
#include <boost/algorithm/string.hpp>

#include "core/common/type.hpp"
#include "core/common/string_server.hpp"
#include "core/hyperquery/query.hpp"

namespace wukong {
static bool isKeyword(char* str, const char* keyword){
    for(int i=0;i<strlen(str);++i) {
        if(str[i]>='A'&&str[i]<='Z'){
            str[i] += 'a'-'A';
        }
    }
    return !strcmp(str,keyword);
}
using namespace boost::archive;

/// Intermediate struct to be placed in yacc %union

class HyperParser {
public:
    enum PatternType { GV, GE, GP, V2E, E2V, E2E_ITSCT, E2E_CT, E2E_IN, V2V, GE_TYPE};
    enum PatternSuffix {Suffix_Dot, Suffix_LArrow, Suffix_RArrow, Suffix_Blank};
    enum ParamType { P_ETYPE, P_VTYPE, P_GE, P_LE, P_GT, P_LT, P_EQ, P_NE, NO_TYPE };

    /// A parsing exception
    struct ParserException {
        /// The message
        std::string message;

        /// Constructor
        ParserException(const std::string &message) : message(message) { }
        /// Constructor
        ParserException(const char *message) : message(message) { }
        /// Destructor
        ~ParserException() { }
    };

    /// An element in a graph pattern
    struct Element {
        /// Possible types
        enum Type { Variable, Literal, IRI, Template, Predicate, TimeStamp, Int };
        /// The type
        Type type;
        /// The literal value
        std::string value;
        /// The id for variables
        ssid_t id;
        /// The value of the timestamp
        int64_t timestamp;
        /// numeric value
        int num;
        /// if is a template element
        bool tplt = false;

        Element(){}

        void clear() {
            type = Variable;
            value = "";
            id = 0;
            timestamp = 0;
        }

        void print_element() const {
            static const char *ElementTypeName[7] = { "Variable", "Literal", "IRI", "Template", "Predicate", "TimeStamp", "Int"};
            logstream(LOG_INFO) << "type: " << ElementTypeName[type] << ", value: ";
            switch (type)
            {
            case Variable:
                logstream(LOG_INFO) << id;
                break;
            case IRI:
                logstream(LOG_INFO) << value;
                break;
            case Int:
                logstream(LOG_INFO) << num;
                break;
            case Literal:
                logstream(LOG_INFO) << value;
                break;
            default:
                ASSERT(false);
            }
            logstream(LOG_INFO) << LOG_endl;
        }
    };
    typedef std::vector<Element> ElementList;

    struct Param {
        ParamType type;
        Element value;
  
        Param(){}
        Param(ParamType type, Element value): type(type), value(value) {}
        ~Param(){}
    };
    typedef std::vector<Param> ParamList;

    /// A graph pattern
    struct Pattern {
        PatternType type;
        // multi input vars and single output var
        ElementList input_vars;
        Element output_var;
        // other parameter of the hyper pattern
        ParamList params;
        /// Constructor
        Pattern(PatternType type, ElementList input, Element output, ParamList params)
            : type(type), input_vars(input), output_var(output), params(params) { }
        /// Destructor
        ~Pattern() {}
    };

    /// A group of patterns
    struct PatternGroup {
        /// The patterns
        std::vector<Pattern> patterns;
    };


private:
    /// The registered prefixes
    std::map<std::string, std::string> prefixes;
    /// The named variables
    std::map<std::string, ssid_t> namedVariables;
    /// The total variable count
    unsigned variableCount;
    /// The named variable count
    unsigned namedVariableCount;

    /// The projection clause
    std::vector<int> projection;
    /// The pattern
    PatternGroup patterns;

    // record pattern meta
    PatternType ty;
    ParamList params;

    /// Lookup or create a named variable
    ssid_t nameVariable(const std::string &name) {
        if (namedVariables.count(name))
            return namedVariables[name];

        variableCount++;
        int result = ++namedVariableCount;
        namedVariables[name] = -result;
        return -result;
    }

public:
    /// Constructor
    explicit HyperParser(): variableCount(0), namedVariableCount(0) {}

    /// Destructor
    ~HyperParser() { }
    /// Clear the parser
    void clear(){
        variableCount = 0;
        namedVariableCount = 0;
        prefixes.clear();
        namedVariables.clear();
        projection.clear();
        patterns.patterns.clear();
    }
    /// Get the patterns
    const PatternGroup &getPatterns() const { return patterns; }
    /// Get the name of a variable
    std::string getVariableName(int id) const {
        for (std::map<std::string, ssid_t>::const_iterator iter = namedVariables.begin(), limit = namedVariables.end();
                iter != limit; ++iter)
            if ((*iter).second == id)
                return (*iter).first;
        return "";
    }

    /// Iterator over the projection clause
    typedef std::vector<int>::const_iterator projection_iterator;
    /// Iterator over the projection
    projection_iterator projectionBegin() const { return projection.begin(); }
    /// Iterator over the projection
    projection_iterator projectionEnd() const { return projection.end(); }
    /// Get the variableCount
    unsigned getVariableCount() const { return variableCount; }


	// Register the new prefix
	void addPrefix(char* name,char* iri){   
        logstream(LOG_DEBUG) << "[HyperParser] add prefix" << LOG_endl; 
        // Cut the real prefix
        int pos = 0;
        for(int i=0;i<strlen(name);++i){
            if(name[i]==':'){
                name[i]='\0';
                break;
            }   
        }

        // remove < >
        iri[strlen(iri)-1]='\0';
        iri = iri+1;  

        if (prefixes.count(std::string(name)))
            throw ParserException("duplicate prefix '" + std::string(name) + "'");
        prefixes[std::string(name)] = iri;
	}

	// Register the projection 
	void addProjection(char* variable){
        logstream(LOG_DEBUG) << "[HyperParser] add projection" << LOG_endl; 
        //remove ?/$
        variable = variable+1;

		projection.push_back(nameVariable(variable));
	}

    // Register PatternGroup in where clause
    void registerPatternGroup(PatternGroup* patternGroup){
        if(patternGroup){
            patterns.patterns = patternGroup->patterns;
            delete patternGroup;
        } else 
            throw ParserException("Unexpected error parsing patternGroup");
    }

    // Make PatternGroup 
    PatternGroup* makePatternGroup(Pattern* pattern, PatternGroup* oldPatternGroup){
        logstream(LOG_DEBUG) << "[HyperParser] make pattern group" << LOG_endl; 
        if (!pattern) throw ParserException("Unexpected error making PatternGroup\n");

        // if the first pattern allocate a new PatternGroup
        PatternGroup* newPatternGroup;
        if(!oldPatternGroup) newPatternGroup = new PatternGroup();
        else newPatternGroup = oldPatternGroup;

        // add pattern into PatternGroup
        newPatternGroup->patterns.insert(newPatternGroup->patterns.begin(), *pattern);

        // free mem
        delete pattern;

        return newPatternGroup;
    }

	// Register the projection 
	Pattern* addPattern(ElementList* inputs, Element* output, int suffix){
        logstream(LOG_DEBUG) << "[HyperParser] add pattern" << LOG_endl; 
    	switch(suffix){
            case Suffix_Dot: case Suffix_Blank:{
                Pattern* p = new Pattern(ty, *inputs, *output, params);
                params.clear();
                delete inputs, output;
                return p;
            }
            case Suffix_LArrow: case Suffix_RArrow: default:
                ASSERT(false);
        }
	}

    // Register meta data in pattern(pattern type + pattern parameters)
    void addPatternMeta(int type, ParamList* params) {
        logstream(LOG_DEBUG) << "[HyperParser] add pattern meta" << LOG_endl; 
        this->ty = static_cast<PatternType>(type);
        if (params) {
            this->params = *params;
            delete params;
        } else 
            this->params.clear();
    }

    ParamList* makeParamList(Param* newParam, ParamList* oldParamList) {
        logstream(LOG_DEBUG) << "[HyperParser] make param list" << LOG_endl; 
        if (!newParam) throw ParserException("Unexpected error making ParamList\n");

        // if the first element allocate a new ElementList
        ParamList* newParamList;
        if(!oldParamList) newParamList = new ParamList();
        else newParamList = oldParamList;

        // add new iri into old list
        newParamList->push_back(*newParam);
        delete newParam;

        return newParamList;        
    }

    Param* makeParam(int type, Element* value) {
        logstream(LOG_DEBUG) << "[HyperParser] make param" << LOG_endl; 
        if (!value) throw ParserException("Unexpected error making Parameter\n");
        Param* param = new Param(static_cast<ParamType>(type), *value);
        delete value;
        return param;
    }

    // make ElementList
    ElementList* makeElementList(Element* newElement, ElementList* oldElementList) {
        logstream(LOG_DEBUG) << "[HyperParser] make element list" << LOG_endl; 
        if (!newElement) throw ParserException("Unexpected error making ElementList\n");

        // if the first element allocate a new ElementList
        ElementList* newElementList;
        if(!oldElementList) newElementList = new ElementList();
        else newElementList = oldElementList;

        // add new iri into old list
        newElementList->push_back(*newElement);
        delete newElement;

        return newElementList;
    }

	// Deal with variable pattern element
	Element* makeVariableElement(char* tokenValue){
        logstream(LOG_DEBUG) << "[HyperParser] make var element" << LOG_endl; 
		Element* result = new Element();
        result->type = Element::Variable;
        tokenValue = tokenValue+1;
        result->id = nameVariable(tokenValue);
        return result;
    }

    // Deal with string element
	Element* makeLiteralElement(char* value, bool tplt){
        logstream(LOG_DEBUG) << "[HyperParser] make literal element" << LOG_endl; 
		Element* result = new Element();

        //remove < >
        value[strlen(value)-1]='\0';
        value = value+1;
        result->value = std::string(value);
        result->type = Element::Literal;
        result->tplt = tplt;

        return result;
	}

	// Deal with iri pattern element
	Element* makeIriElement(char* iriValue, bool tplt){
        logstream(LOG_DEBUG) << "[HyperParser] make iri element" << LOG_endl; 
		Element* result = new Element();

        //remove < >
        iriValue[strlen(iriValue)-1]='\0';
        iriValue = iriValue+1;
        result->value = std::string(iriValue);
        result->type = Element::IRI;
        result->tplt = tplt;

        return result;
	}

	// Deal with alias iri pattern element
	Element* makePrefixIriElement(char* prefix,char* suffix,bool tplt){
        logstream(LOG_DEBUG) << "[HyperParser] add prefix iri element" << LOG_endl; 
		Element* result=new Element();
        // Cut the real prefix
        int pos = 0;
        for(int i=0;i<strlen(prefix);++i){
            if(prefix[i]==':'){
                prefix[i]='\0';
                break;
            }   
        }
        if(!prefixes.count(prefix)){
            throw ParserException("unknown prefix \'" + std::string(prefix) + "\'");
        }
        const char* newPrefix=prefixes[prefix].c_str();
        result->value = std::string(newPrefix);
        result->value += std::string(suffix);
        result->type = Element::IRI;
        result->tplt = tplt;

        return result;
    }

    // Deal with int parameters
    Element* makeIntElement(int num) {
        logstream(LOG_DEBUG) << "[HyperParser] make num element" << LOG_endl; 
		Element* result = new Element();
        result->type = Element::Int;
        result->num = num;
        return result;
    }

    // post parsing operations
    void postParsing(){
        // Fixup empty projections (i.e. *)
        if (!projection.size()) {
            for (std::map<std::string, ssid_t>::const_iterator iter = namedVariables.begin(), limit = namedVariables.end();
                    iter != limit; ++iter)
                projection.push_back((*iter).second);
        }
    }

};

} // namespace wukong
