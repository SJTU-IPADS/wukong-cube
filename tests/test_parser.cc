#include <gtest/gtest.h>
#include "core/hyperquery/parser.hpp"

#define HYPER_QUERY_SINGLE_V2E "hyper_query/test/basic/v2e/singleV_hq"
#define HYPER_QUERY_MUL_V2E "hyper_query/test/basic/v2e/multiV_hq"
#define HYPER_QUERY_SINGLE_E2V "hyper_query/test/basic/e2v/singleE_hq"
#define HYPER_QUERY_MUL_E2V "hyper_query/test/basic/e2v/multiE_hq"
#define HYPER_QUERY_CT_E2E "hyper_query/test/basic/e2e/contain_hq"
#define HYPER_QUERY_IN_E2E "hyper_query/test/basic/e2e/in_hq"
#define HYPER_QUERY_ITSCT_E2E "hyper_query/test/basic/e2e/intersect_hq"
#define HYPER_QUERY_ITSCT_V2V "hyper_query/test/basic/v2v/intersect_hq"

#define HTYPE 5
#define HID1 10
#define HID2 11
#define VID1 20
#define VID2 21

namespace test {
using namespace wukong;

class ParserTest : public ::testing::Test {
    protected:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    ParserTest() : parser(&str_server) {
        // initialize string server
        str_server.add("<http://www.w3.org/1999/02/22-rdf-syntax-ns#CoAuthor>", HTYPE);
        str_server.add("<http://www.edu/Professor0>", VID1);
        str_server.add("<http://www.edu/Professor1>", VID2);
        str_server.add_he("<http://www.edu/CoAuthor0>", HID1);
        str_server.add_he("<http://www.edu/CoAuthor1>", HID2);
    }

    ~ParserTest() {}

    // You can define per-test set-up and tear-down logic as usual.
    virtual void SetUp() {}

    virtual void TearDown() {}
  
    StringServer str_server;
    Parser parser;
};

TEST_F(ParserTest, V2E) {
    HyperQuery request1, request2;
    uint64_t start, end;

    // Parse the SPARQL query single v2e
    logstream(LOG_INFO) << "-----test single v2e-----" << LOG_endl;
    start = timer::get_usec();
    int ret = parser.parse(HYPER_QUERY_SINGLE_V2E, request1);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request1.pattern_group.print_group();
    ASSERT_EQ(request1.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].type, HyperQuery::V2E);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids[0], VID1);
    ASSERT_EQ(request1.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request1.result.required_vars.size(), 1);

    // Parse the SPARQL query multi v2e
    logstream(LOG_INFO) << "-----test multi v2e-----" << LOG_endl;
    start = timer::get_usec();
    ret = parser.parse(HYPER_QUERY_MUL_V2E, request2);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request2.pattern_group.print_group();
    ASSERT_EQ(request2.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].type, HyperQuery::V2E);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vids.size(), 2);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vids[0], VID2);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vids[1], VID1);
    ASSERT_EQ(request2.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request2.pattern_group.patterns[0].params.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request2.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request2.result.required_vars.size(), 1);
}

TEST_F(ParserTest, E2V) {
    HyperQuery request1, request2;
    uint64_t start, end;

    // Parse the SPARQL query single e2v
    logstream(LOG_INFO) << "-----test single e2v-----" << LOG_endl;
    start = timer::get_usec();
    int ret = parser.parse(HYPER_QUERY_SINGLE_E2V, request1);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request1.pattern_group.print_group();
    ASSERT_EQ(request1.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].type, HyperQuery::E2V);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids[0], HID1);
    ASSERT_EQ(request1.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params.size(), 0);
    ASSERT_EQ(request1.result.required_vars.size(), 1);

    // Parse the SPARQL query multi e2v
    logstream(LOG_INFO) << "-----test multi e2v-----" << LOG_endl;
    start = timer::get_usec();
    ret = parser.parse(HYPER_QUERY_MUL_E2V, request2);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request2.pattern_group.print_group();
    ASSERT_EQ(request2.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].type, HyperQuery::E2V);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids.size(), 2);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vids.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids[0], HID2);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids[1], HID1);
    ASSERT_EQ(request2.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request2.pattern_group.patterns[0].params.size(), 0);
    ASSERT_EQ(request2.result.required_vars.size(), 1);
}

TEST_F(ParserTest, E2E) {
    HyperQuery request1, request2, request3;
    uint64_t start, end;

    // Parse the SPARQL query contain e2e
    logstream(LOG_INFO) << "-----test contain e2e-----" << LOG_endl;
    start = timer::get_usec();
    int ret = parser.parse(HYPER_QUERY_CT_E2E, request1);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request1.pattern_group.print_group();
    ASSERT_EQ(request1.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].type, HyperQuery::E2E_CT);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids[0], HID1);
    ASSERT_EQ(request1.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request1.result.required_vars.size(), 1);

    // Parse the SPARQL query in e2e
    logstream(LOG_INFO) << "-----test in e2e-----" << LOG_endl;
    start = timer::get_usec();
    ret = parser.parse(HYPER_QUERY_IN_E2E, request2);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request2.pattern_group.print_group();
    ASSERT_EQ(request2.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].type, HyperQuery::E2E_IN);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_vids.size(), 0);
    ASSERT_EQ(request2.pattern_group.patterns[0].input_eids[0], HID1);
    ASSERT_EQ(request2.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request2.pattern_group.patterns[0].params.size(), 1);
    ASSERT_EQ(request2.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request2.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request2.result.required_vars.size(), 1);

    // Parse the SPARQL query intersect e2e
    logstream(LOG_INFO) << "-----test intersect e2e-----" << LOG_endl;
    start = timer::get_usec();
    ret = parser.parse(HYPER_QUERY_ITSCT_E2E, request3);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request3.pattern_group.print_group();
    ASSERT_EQ(request3.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request3.pattern_group.patterns[0].type, HyperQuery::E2E_ITSCT);
    ASSERT_EQ(request3.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request3.pattern_group.patterns[0].input_eids.size(), 1);
    ASSERT_EQ(request3.pattern_group.patterns[0].input_vids.size(), 0);
    ASSERT_EQ(request3.pattern_group.patterns[0].input_eids[0], HID1);
    ASSERT_EQ(request3.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request3.pattern_group.patterns[0].params.size(), 2);
    ASSERT_EQ(request3.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request3.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request3.pattern_group.patterns[0].params[1].type, INT_t);
    ASSERT_EQ(request3.pattern_group.patterns[0].params[1].num, 2);
    ASSERT_EQ(request3.result.required_vars.size(), 1);
}

TEST_F(ParserTest, V2V) {
    HyperQuery request1;
    uint64_t start, end;

    // Parse the SPARQL query intersect v2v
    logstream(LOG_INFO) << "-----test intersect v2v-----" << LOG_endl;
    start = timer::get_usec();
    int ret = parser.parse(HYPER_QUERY_ITSCT_V2V, request1);
    if (ret) ASSERT_ERROR_CODE(false, ret);
    end = timer::get_usec();
    logstream(LOG_INFO) << "Parsing time: " << (end - start) << " usec" << LOG_endl;

    // assure correctness
    request1.pattern_group.print_group();
    ASSERT_EQ(request1.pattern_group.patterns.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].type, HyperQuery::V2V);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vars.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_eids.size(), 0);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids.size(), 1);
    ASSERT_EQ(request1.pattern_group.patterns[0].input_vids[0], VID1);
    ASSERT_EQ(request1.pattern_group.patterns[0].output_var, -1);
    ASSERT_EQ(request1.pattern_group.patterns[0].params.size(), 2);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].type, SID_t);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[0].sid, HTYPE);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[1].type, INT_t);
    ASSERT_EQ(request1.pattern_group.patterns[0].params[1].num, 2);
    ASSERT_EQ(request1.result.required_vars.size(), 1);
}

}
