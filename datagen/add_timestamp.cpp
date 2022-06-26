/*
 * Copyright (c) 2021 Shanghai Jiao Tong University.
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

#include <string>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <ctime>
#include <cstdlib>
#include <sys/types.h>
#include <dirent.h>
using namespace std;

int main(int argc, char** argv) {
    if (argc != 2) {
        printf("usage: ./add_timestamp id_triples_directory_name\n");
        return -1;
    }

    DIR *dir = opendir(argv[1]);
    if (dir == NULL) {
        cout << "failed to open directory" << argv[1];
        return -1;
    }

    unsigned seed = time(0);
    srand(seed);

    int64_t subject, predicate, object;

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        string fname = string(argv[1]) + "/" + ent->d_name;
        if (string(ent->d_name).find("id") != string::npos && string(ent->d_name).find("nt") != string::npos) {
            cout << "Processing: " << ent->d_name << endl;
            ifstream infile;
            infile.open(fname);

            ofstream outfile;
            string dst = string(argv[1]) + "/" + ent->d_name + "1";
            outfile.open(dst, ios::out | ios::trunc);
            while (infile >> subject >> predicate >> object) {
                outfile << subject << "\t" << predicate << "\t" << object << "\t" << rand() % seed + 1 << "\t" << rand() % seed + 1 << endl;
            }
            infile.close();
            outfile.close();
            remove(fname.c_str());
            rename(dst.c_str(),fname.c_str());
        }
    }

    cout << "Finished!" << endl;
}