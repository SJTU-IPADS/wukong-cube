/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_wukong_WukongGraph */

#ifndef _Included_com_wukong_WukongGraph
#define _Included_com_wukong_WukongGraph
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_wukong_WukongGraph
 * Method:    retrieveClusterInfo
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_wukong_WukongGraph_retrieveClusterInfo
  (JNIEnv *, jobject);

/*
 * Class:     com_wukong_WukongGraph
 * Method:    executeSparqlQuery
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_wukong_WukongGraph_executeSparqlQuery
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_wukong_WukongGraph
 * Method:    connectToServer
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_com_wukong_WukongGraph_connectToServer
  (JNIEnv *, jobject, jstring, jint);

/*
 * Class:     com_wukong_WukongGraph
 * Method:    disconnectToServer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_wukong_WukongGraph_disconnectToServer
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
