#include <string>

#include "WukongGraph.h"
#include "client/rpc_client.hpp"

using namespace wukong;

inline std::string ConvertToString(JNIEnv* env, jstring jstr) {
  const char* cstr = env->GetStringUTFChars(jstr, NULL);
  std::string sstr = cstr;
  env->ReleaseStringUTFChars(jstr, cstr);
  return sstr;
}

inline jstring ConvertToJString(JNIEnv* env, std::string str) {
  return env->NewStringUTF(str.c_str());
}

JNIEXPORT
jlong JNICALL Java_com_wukong_WukongGraph_connectToServer(JNIEnv *env, jobject obj, jstring address, jint port) {
    std::string addr_str = ConvertToString(env, address);

    RPCClient* client = new RPCClient();
    client->connect_to_server(addr_str, port);

    return (jlong)(client);
}

JNIEXPORT
void JNICALL Java_com_wukong_WukongGraph_disconnectToServer(JNIEnv *env, jobject obj, jlong native_handle) {
    RPCClient* client = (RPCClient*) native_handle;
    if(client) delete client;
}

JNIEXPORT
void JNICALL Java_com_wukong_WukongGraph_retrieveClusterInfo(JNIEnv* env, jobject obj) {
    // get handle
    jclass wukong_class = env->GetObjectClass(obj);
    jfieldID handleFieldId = env->GetFieldID(wukong_class, "native_client_handle", "J");
    RPCClient* client_handle = (RPCClient*)(env->GetObjectField(obj, handleFieldId));

    client_handle->retrieve_cluster_info();
    return;
}

JNIEXPORT
jstring JNICALL Java_com_wukong_WukongGraph_executeSparqlQuery(JNIEnv *env, jobject obj, jstring query) {
    // get handle
    jclass wukong_class = env->GetObjectClass(obj);
    jfieldID handleFieldId = env->GetFieldID(wukong_class, "native_client_handle", "J");
    RPCClient* client_handle = (RPCClient*)(env->GetObjectField(obj, handleFieldId));

    std::string query_str = ConvertToString(env, query);
    std::string result_str;

    client_handle->execute_sparql_query(query_str, result_str);

    return ConvertToJString(env, result_str);
}