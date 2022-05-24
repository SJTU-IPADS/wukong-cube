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

class SPARQLParser {
public:
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
        enum Type { Variable, Literal, IRI, Template, Predicate, TimeStamp, Invalid };
        /// Possible sub-types for literals
        enum SubType { None, CustomLanguage, CustomType };
        /// The type
        Type type;
        /// The sub-type
        SubType subType;
        /// The value of the sub-type
        std::string subTypeValue;
        /// The literal value
        std::string value;
        /// The id for variables
        ssid_t id;
        /// The value of the timestamp
        int64_t timestamp;

        Element(){}
    };

    /// A graph pattern
    struct Pattern {
        /// The entires
        Element subject, predicate, object, ts, te;
        Pattern(Element subject, Element predicate, Element object, Element ts, Element te)
            : subject(subject), predicate(predicate), object(object), ts(ts), te(te) { }
        /// Direction
        dir_t direction = OUT;
        /// Constructor
        Pattern(Element subject, Element predicate, Element object)
            : subject(subject), predicate(predicate), object(object) { }
        /// Destructor
        ~Pattern() { }
    };

    /// A filter entry
    struct Filter {
        /// Possible types
        enum Type {
            Or, And, Equal, NotEqual, Less, LessOrEqual, Greater,
            GreaterOrEqual, Plus, Minus, Mul, Div, Not, UnaryPlus, UnaryMinus,
            Literal, Variable, IRI, Function, ArgumentList, Builtin_str,
            Builtin_lang, Builtin_langmatches, Builtin_datatype, Builtin_bound,
            Builtin_sameterm, Builtin_isiri, Builtin_isblank, Builtin_isliteral,
            Builtin_regex, Builtin_in
        };

        /// The type
        Type type;
        /// Input arguments
        Filter *arg1, *arg2, *arg3;
        /// The value (for constants)
        std::string value;
        /// The type (for constants)
        std::string valueType;
        /// Possible subtypes or variable ids
        int valueArg;
        /// Constructor
        Filter() : arg1(0), arg2(0), arg3(0), valueArg(0), type(Or){ }
        /// Copy-Constructor
        Filter(const Filter &other)
            : type(other.type), arg1(0), arg2(0), arg3(0),
              value(other.value), valueType(other.valueType),
              valueArg(other.valueArg) {
            if (other.arg1)
                arg1 = new Filter(*other.arg1);
            if (other.arg2)
                arg2 = new Filter(*other.arg2);
            if (other.arg3)
                arg3 = new Filter(*other.arg3);
        }
        /// Destructor
        ~Filter() {
            delete arg1;
            delete arg2;
            delete arg3;
        }

        /// Assignment
        Filter &operator=(const Filter &other) {
            if (this != &other) {
                type = other.type;
                delete arg1;
                if (other.arg1)
                    arg1 = new Filter(*other.arg1);
                else
                    arg1 = 0;

                delete arg2;
                if (other.arg2)
                    arg2 = new Filter(*other.arg2);
                else
                    arg2 = 0;

                delete arg3;
                if (other.arg3)
                    arg3 = new Filter(*other.arg3);
                else
                    arg3 = 0;

                value = other.value;
                valueType = other.valueType;
                valueArg = other.valueArg;
            }
            return *this;
        }
    };

    /// A group of patterns
    struct PatternGroup {
        /// The patterns
        std::vector<Pattern> patterns;
        /// The filter conditions
        std::vector<Filter> filters;
        /// The optional parts
        std::vector<PatternGroup> optional;
        /// The union parts
        std::vector<PatternGroup> unions;
    };
    
    enum NumericType {Numeric_Integer, Numeric_Decimal, Numeric_Double};

    enum QueryType {Type_Select, Type_Ask};

    enum PatternType { Type_Pattern,Type_Filter,
                       Type_Optional,Type_PatternGroup,Type_Union
                     };

    enum VariableOrder {Order_Asc, Order_Desc};

    enum PatternSuffix {Suffix_Dot, Suffix_LArrow, Suffix_RArrow, Suffix_Blank};

    /// The projection modifier
    enum ProjectionModifier { Modifier_None, Modifier_Distinct,
                              Modifier_Reduced, Modifier_Count,
                              Modifier_Duplicates
                            };
    /// Sort order
    struct Order {
        /// Variable id
        int id;
        /// Desending
        bool descending;
    };

    struct PatternNode {
        PatternType type; 
        Pattern* pattern;
        Filter* filter;
        PatternGroup* patternGroup;
        std::vector<PatternGroup>* unionGroup;
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

    /// The projection modifier
    ProjectionModifier projectionModifier;
    /// The projection clause
    std::vector<int> projection;
    /// The pattern
    PatternGroup patterns;
    /// The sort order
    std::vector<Order> order;
    /// The result limit, -1 means no limit
    int limit;
    /// The result offset
    unsigned offset;
    // indicate if custom grammar is in use
    bool usingCustomGrammar;
    bool corun_enabled;
    int corun_step;
    int fetch_step;

    // indicate query type(select or ask)
    QueryType q_type;

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
    int64_t ts = TIMESTAMP_MIN; // start timestamp after keyword FROM
    int64_t te = TIMESTAMP_MAX; // end timestamp after keyword FROM
    /// Constructor
    explicit SPARQLParser()
        : variableCount(0), namedVariableCount(0),
          projectionModifier(Modifier_None), limit(-1), offset(0u) {
        usingCustomGrammar = false;
        corun_enabled = false;
        corun_step = 0;
        fetch_step = 0;
    }
    /// Destructor
    ~SPARQLParser() { }
    /// Clear the parser
    void clear(){
        variableCount = 0;
        namedVariableCount = 0;
        projectionModifier = Modifier_None;
        limit = -1;
        offset = 0u;
        usingCustomGrammar = false;
        corun_enabled = false;
        corun_step = 0;
        fetch_step = 0;
        ts = TIMESTAMP_MIN;
        te = TIMESTAMP_MAX;
        prefixes.clear();
        namedVariables.clear();
        projection.clear();
        order.clear();
        patterns.patterns.clear();
        patterns.filters.clear();
        patterns.unions.clear();
        patterns.optional.clear();
    }
    /// Get the query type
    const QueryType getQueryType() const {return q_type;}
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

    /// Iterator over the order by clause
    typedef std::vector<Order>::const_iterator order_iterator;
    /// Iterator over the order by clause
    order_iterator orderBegin() const { return order.begin(); }
    /// Iterator over the order by clause
    order_iterator orderEnd() const { return order.end(); }

    /// The projection modifier
    ProjectionModifier getProjectionModifier() const { return projectionModifier; }
    /// The size limit
    int getLimit() const { return limit; }
    /// The offset
    unsigned getOffset() const { return offset; }
    /// Get the variableCount
    unsigned getVariableCount() const { return variableCount; }
    // indicate if custom grammar is in use
    bool isUsingCustomGrammar() const { return usingCustomGrammar; }
    // indicate if corun optimization is in use
    bool isCorunEnabled() const { return corun_enabled; }
    // get the corun step
    int getCorunStep() const { return corun_step; }
    // get the fetch step
    int getFetchStep() const { return fetch_step; }

    // from snapshot <>
    void parseFromSnapshot(char* datetime){
        std::string datetime_str(datetime);
        datetime_str = datetime_str.substr(0, 19);
        ts = wukong::time_tool::str2int(datetime_str);
        te = ts;
    }

    // from []
    void parseFromTime(char* start_time, char* end_time){
        end_time[strlen(end_time) - 1] = '\0';
        std::string ts_str(start_time);
        ts_str = ts_str.substr(0, 19);
        std::string te_str(end_time);
        te_str = te_str.substr(0, 19);
        ts = wukong::time_tool::str2int(ts_str);
        te = wukong::time_tool::str2int(te_str);
    }

    // Register the query type
    void registerQueryType(int type) {
        q_type = static_cast<SPARQLParser::QueryType>(type);
    }

	// Register the new prefix
	void addPrefix(char* name,char* iri){  
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

	// Register the projection modifier
	void addProjectionModifier(char* pm){
        if(isKeyword(pm,"distinct")){
            projectionModifier = Modifier_Distinct;
        }
        else if(isKeyword(pm,"reduced")){
            projectionModifier = Modifier_Reduced;
        }
        else if(isKeyword(pm,"count")){
            projectionModifier = Modifier_Count;
        }
        else if(isKeyword(pm,"duplicates")){
            projectionModifier = Modifier_Duplicates;
        }else{
            /* do nothing, align with original implementation */
        }
	}

	// Register the projection 
	void addProjection(char* variable){
        //remove ?/$
        variable = variable+1;

		projection.push_back(nameVariable(variable));
	}

    // Register PatternGroup in where clause
    void registerPatternGroup(PatternGroup* patternGroup){
        if(patternGroup){
            patterns.patterns = patternGroup->patterns;
            patterns.filters = patternGroup->filters;
            patterns.optional = patternGroup->optional;
            patterns.unions = patternGroup->unions;
            delete patternGroup;
        }else{
            throw ParserException("Unexpected error parsing patternGroup");
        }
    }

    // Make PatternGroup 
    PatternGroup* makePatternGroup(PatternNode* patternNode, PatternGroup* oldPatternGroup){
        PatternGroup* newPatternGroup;
        if(!oldPatternGroup){
            newPatternGroup = new PatternGroup();
        }else{
            newPatternGroup = oldPatternGroup;
        }
        if(!patternNode){
            throw ParserException("Unexpected error making PatternGroup\n");
        }
        switch(patternNode->type){
            case Type_Pattern:{
                (newPatternGroup->patterns).push_back(*(patternNode->pattern));
                delete patternNode->pattern;
                break;
            }
            case Type_Filter:{
                (newPatternGroup->filters).push_back(*(patternNode->filter));
                delete patternNode->filter;
                break;
            }
            case Type_Optional:{
                (newPatternGroup->optional).push_back(*(patternNode->patternGroup));
                delete patternNode->filter;
                break;
            } 
            case Type_Union:{
                std::vector<PatternGroup>* pgList = patternNode->unionGroup; 
                if(pgList->size()==1){
                    newPatternGroup->patterns.insert(newPatternGroup->patterns.end(),(*pgList)[0].patterns.begin(),(*pgList)[0].patterns.end());
                    newPatternGroup->filters.insert(newPatternGroup->filters.end(),(*pgList)[0].filters.begin(),(*pgList)[0].filters.end());
                    newPatternGroup->optional.insert(newPatternGroup->optional.end(),(*pgList)[0].optional.begin(),(*pgList)[0].optional.end());
                    newPatternGroup->unions.insert(newPatternGroup->unions.end(),(*pgList)[0].unions.begin(),(*pgList)[0].unions.end());
                }else if(pgList->size()>1){
                    for(auto pg : *pgList){
                        newPatternGroup->unions.push_back(pg);
                    }
                }else{
                    throw ParserException("Unexpected error making union patternNode\n");
                }
            }
        }
        delete patternNode;
        return newPatternGroup;
    }

    // Make an vector of PatternGroup
    std::vector<PatternGroup>* makePatternGroupList(PatternGroup* pg, std::vector<PatternGroup>* pgList){
        std::vector<PatternGroup>* newPgList;
        if(!pgList){
            newPgList = new std::vector<PatternGroup>();
        }else{
            newPgList = pgList;
        }
        newPgList->push_back(*pg);
        delete pg;
        return newPgList;
    }

    // Register PatternList in optional clause
    void registerOptionalPatternList(std::vector<Pattern>* patternList){
        PatternGroup new_group;
        if(patternList){
            new_group.patterns = *patternList;
            delete patternList;
            patterns.optional.push_back(new_group);
        }
    }

    // Build PatternList
    PatternNode* makePatternNode(int type, Pattern* p, Filter* f, PatternGroup* pg,std::vector<PatternGroup>* pgList){
        PatternNode* pn = new PatternNode();
        pn->type = static_cast<wukong::SPARQLParser::PatternType>(type);
        switch(type){
            case Type_Pattern:{
                pn->pattern = p;
                break;
            }
            case Type_Filter:{
                pn->filter = f;
                break;
            }
            case Type_Optional:{
                pn->patternGroup = pg;
                break;
            }
            case Type_Union:{
                pn->unionGroup = pgList;
                break;
            }
            default:{
                throw ParserException("Unexpected error parsing patternNode");
            }
        }
        return pn;
    }

	// Register the projection 
	Pattern* addPattern(Element* subject, Element* predicate, Element* object, int suffix, Element* ts, Element* te){
    	switch(suffix){
            case Suffix_Dot: case Suffix_Blank:{
                Pattern* p = new Pattern(*subject, *predicate, *object, *ts, *te);
                delete subject, predicate, object, ts, te;
                return p;
            }
            case Suffix_LArrow:{
                usingCustomGrammar = true;
                Pattern* p = new Pattern(*object, *predicate, *subject, *ts, *te);
                p->direction = IN;
                delete subject,predicate,object, ts, te;
                return p;
            }
            case Suffix_RArrow:{
                usingCustomGrammar = true;
                Pattern* p = new Pattern(*subject, *predicate, *object, *ts, *te);
                delete subject,predicate,object, ts, te;
                return p;
            }
            default:{
                printf("Unidentified pattern suffix!");
                return NULL;
            }
        }
	}

    // Register the order
    void addOrder(char* variable, int order_ = Order_Asc){
        Order o;
        if(order_ == Order_Asc){
            o.descending = false;
        }else{
            o.descending = true;
        }
        if(!variable){
            o.id = ~0u;
        }else if(variable[0]!='?'&&variable[0]!='$'){
            throw ParserException("variable expected in order-by clause");
        }else{
            variable = variable+1;
            for(int i=0;i<strlen(variable);++i){
                if(variable[i]==')'){
                    variable[i]='\0';
                    break;
                }   
            }
            o.id = nameVariable(variable);
        }
        order.push_back(o);
    }

    void addLimit(int limit_){
        limit = limit_;
    }

    void addOffset(int offset_){
        offset = offset_;
    }

	// Deal with variable pattern element
	Element* makeVariableElement(char* tokenValue){
		Element* result = new Element();
        result->type = Element::Variable;
        tokenValue = tokenValue+1;
        result->id = nameVariable(tokenValue);
        return result;
    }

    // Deal with predicate pattern element
    Element* makePredicateElement(){
		Element* result = new Element();
        result->type = Element::Predicate;
        return result;
    }

    // Deal with anon pattern element
    Element* makeAnonElement(){
		Element* result = new Element();
        result->type = Element::Variable;
        result->id = variableCount++;
        return result;
    }

	// Deal with iri pattern element
	Element* makeIriElement(char* iriValue, bool customGrammar){
		Element* result = new Element();
        //remove < >
        iriValue[strlen(iriValue)-1]='\0';
        iriValue = iriValue+1;
        result->value = std::string(iriValue);
        if(customGrammar){
            usingCustomGrammar = true;
            result->type = Element::Template;
        }else{
            result->type = Element::IRI;
        }
        return result;
	}

    // Deal with Timestamp pattern element
	Element* makeTimestampElement(char* datetimeValue){
		Element* result = new Element();
        std::string str = std::string(datetimeValue);
        result->timestamp = wukong::time_tool::str2int(str);
        result->type = Element::TimeStamp;
        return result;
	}

    Element* makeInvalidTimestampElement(){
		Element* result = new Element();
        result->type = Element::Invalid;
        return result;
	}

	// Deal with String pattern element
	Element* makeStringElement(char* stringValue,char* customLanguage=NULL,char* customType=NULL){
		Element* result=new Element();
        result->type = Element::Literal;
        // remove " "
        stringValue = stringValue+1;
        for(int i=0;i<strlen(stringValue);++i){
            if(stringValue[i]=='\"'){
                stringValue[i]='\0';
                break;
            }   
        }
        result->value = std::string(stringValue); 
        if(customLanguage){
            result->subType = Element::CustomLanguage;
            result->subTypeValue = std::string(customLanguage);
        }
        if(customType){
            result->subType = Element::CustomType;
            result->subTypeValue = std::string(customType);
        }
        return result;
	}

	// Deal with alias iri pattern element
	Element* makePrefixIriElement(char* prefix,char* suffix,bool customGrammar){
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
        if(customGrammar){
            usingCustomGrammar = true;
            result->type = Element::Template;
        }else{
            result->type = Element::IRI;
        }
        return result;
    }

    // Make an vector of expression
    std::vector<Filter*>* makeExpList(Filter* exp, std::vector<Filter*>* expList){
        std::vector<Filter*>* newExpList;
        if(!expList){
            newExpList = new std::vector<Filter*>();
        }else{
            newExpList = expList;
        }
        newExpList->push_back(exp);
        return newExpList;
    }

    /// Parse a builtin function
    Filter *parseBuiltInCall(char* funcName,std::vector<Filter*>* argList,char* variable){
        for(int i=0;i<strlen(funcName);++i){
            if(funcName[i]=='('){
                funcName[i]='\0';
            } 
        }
        std::unique_ptr<Filter> result(new Filter);
        // func type func(variable)
        if(!argList){
            result->type = Filter::Builtin_bound;
            std::unique_ptr<Filter> arg(new Filter());
            arg->type = Filter::Variable;
            variable +=1;
            for(int i=0;i<strlen(variable);++i){
                if(variable[i]==')'){
                    variable[i]='\0';
                }
            }
            arg->valueArg = nameVariable(variable);
            result->arg1 = arg.release();
        }
        // func type func(Exp)
        else if(argList->size()==1){
            result->arg1 = (*argList)[0];
            if(isKeyword(funcName,"str")){
                result->type = Filter::Builtin_str;
            }else if(isKeyword(funcName,"lang")){
                result->type = Filter::Builtin_lang;
            }else if(isKeyword(funcName,"datatype")){
                result->type = Filter::Builtin_datatype;
            }else if(isKeyword(funcName,"isiri")){
                result->type = Filter::Builtin_isiri;
            }else if(isKeyword(funcName,"isuri")){
                result->type = Filter::Builtin_isiri;
            }else if(isKeyword(funcName,"isblank")){
                result->type = Filter::Builtin_isblank;
            }else if(isKeyword(funcName,"isliteral")){
                result->type = Filter::Builtin_isliteral;
            }
        }
        // func type func(Exp,Exp)
        else if(argList->size()==2){
            result->arg1 = (*argList)[1];
            result->arg2 = (*argList)[0];
            if(isKeyword(funcName,"langmatches")){
                result->type = Filter::Builtin_langmatches;
            }else if(isKeyword(funcName,"sameterm")){
                result->type = Filter::Builtin_sameterm;
            }else if(isKeyword(funcName,"regex")){
                result->type = Filter::Builtin_regex;
            }
        }
        // func type func(Exp,Exp,Exp)
        else if(argList->size()==3){
            result->arg1 = (*argList)[2];
            result->arg2 = (*argList)[1];
            result->arg3 = (*argList)[0];
            if(isKeyword(funcName,"regex")){
                result->type = Filter::Builtin_regex;
            }
        }
        // func in
        if(isKeyword(funcName,"in")&&argList->size()>=1){
            result->type = Filter::Builtin_in;
            result->arg1 = (*argList)[argList->size()-1];
            if(argList->size()>=2){
                std::unique_ptr<Filter> args(new Filter);
                Filter* tail = args.get();
                tail->type = Filter::ArgumentList;
                tail->arg1 = (*argList)[argList->size()-2];
                for(int i=2;i<argList->size();++i){
                    tail = tail->arg2 = new Filter;
                    tail->type = Filter::ArgumentList;
                    tail->arg1 = (*argList)[argList->size()-i-1];
                }
                result->arg2 = args.release();
            }
        }
        // impossible
        if(result->type==0){
            throw ParserException("unknown function '" + std::string(funcName) + "'");
        }
        if(argList) delete argList;
        return result.release();
    }

    /// Parse an RDF Literal
    Filter* parseRDFLiteral(char* stringValue,char* customLanguage=NULL,char* customType=NULL){
        Element* p = makeStringElement(stringValue,customLanguage,customType);
        std::unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        result->value = p->value;
        result->valueType = p->subTypeValue;
        result->valueArg = p->subType;
        delete p;
        return result.release();
    }
    
    /// Parse a bool literal
    Filter* parseBoolLiteral(bool p){
        std::unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        if(p){
            result->value = "true";
        }else{
            result->value = "false";
        }
        result->valueType = "http://www.w3.org/2001/XMLSchema#boolean";
        result->valueArg = Element::CustomType;
        return result.release();
    }

    /// parse an variable
    Filter* parseVariable(char* variable){
        int pos;
        variable+=1;
        for(int i=0;i<strlen(variable);++i){
            char c = variable[i]; 
            if (((c >= '0') && (c <= '9'))
                || ((c >= 'A') && (c <= 'Z'))
                || ((c >= 'a') && (c <= 'z'))) 
            {
                continue;
            }else{
                variable[i] = '\0';
            }
        }
        std::unique_ptr<Filter> result(new Filter);
        result->type = Filter::Variable;
        result->value = std::string(variable);
        result->valueArg = nameVariable(result->value);
        return result.release();
    }

    /// parse a numeric literal
    Filter* parseNumericLiteral(char* value, int type){
        std::unique_ptr<Filter> result(new Filter);
        result->type = Filter::Literal;
        result->value = value;
        result->valueArg = Element::CustomType;
        switch(type){
            case Numeric_Integer:{
                result->valueType = "http://www.w3.org/2001/XMLSchema#integer";
                break;
            }
            case Numeric_Double:{
                result->valueType = "http://www.w3.org/2001/XMLSchema#double";
                break;
            }
            case Numeric_Decimal:{
                result->valueType = "http://www.w3.org/2001/XMLSchema#decimal";
                break;
            }
            default:{
                throw;
            }
        }
        return result.release();
    }

    /// Parse IRI expression
    Filter* parseIRIOrFunc(char* iri, std::vector<Filter*>* argList){
        std::unique_ptr<Filter> result(new Filter);
        result->type = Filter::IRI;
        //remove < >
        iri[strlen(iri)-1]='\0';
        iri = iri+1;
        result->value = std::string(iri);
        if(argList){
            std::unique_ptr<Filter> call(new Filter);
            call->type = Filter::Function;
            call->arg1 = result.release();
            if(argList->size()>0){
                std::unique_ptr<Filter> args(new Filter);
                Filter* tail = args.get();
                tail->type = Filter::ArgumentList;
                tail->arg1 = (*argList)[argList->size()-1];
                for(int i=1;i<argList->size();++i){
                    tail = tail->arg2 = new Filter;
                    tail->type = Filter::ArgumentList;
                    tail->arg1 = (*argList)[argList->size()-i-1];
                }   
            }
            result = std::move(call);
        }
        return result.release();        
    }

    /// Parse unary expression
    Filter *parseUnaryExp(Filter* left, int operand){
        std::unique_ptr<Filter> result(new Filter);
        result->type = static_cast<Filter::Type>(operand);
        result->arg1 = left;
        return result.release();        
    }

    /// Parse two-sides expression
    Filter *parseDualExp(Filter* left, Filter* right,int operand){
        if(!right){
            return left;
        }
        std::unique_ptr<Filter> newEntry(new Filter);
        newEntry->type = static_cast<Filter::Type>(operand);
        newEntry->arg1 = left;
        newEntry->arg2 = right;
        return newEntry.release();
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
