#include<iostream>
#include<fstream>
#include<sstream>
#include<utility>
#include<map>
#include<format>
#include<unordered_map>
#include <cctype>
#include <algorithm>
using namespace std;

void write_intermediate_inverted_index(
    int file_index, map<string,
    vector<pair<string, long>>> inverted_index
) {
    string file_name = format("intermediate_inverted_indices/intermediate_inverted_index_{}.txt", file_index);
    ofstream intermediate_inverted_index(file_name);
    for (const auto& term : inverted_index)
    {
        intermediate_inverted_index << term.first << ":";
        for (const auto& document_term : term.second)
        {
            intermediate_inverted_index << document_term.first << " " << document_term.second;
            if (&document_term != &term.second.back())
            {
                intermediate_inverted_index << " ";
            }
        }
        intermediate_inverted_index << endl;
    }
    intermediate_inverted_index.close();
    cout << "Intermediate Inverted Index " << file_index << " file saved" << endl;
}

void update_inverted_index(
    string doc_id,
    map<string, vector<pair<string, long>>>& intermediate_inverted_index,
    unordered_map<string, long> terms_per_document
) {
    for (const auto& term : terms_per_document)
    {
        pair<string, long> document_term = {doc_id, term.second};
        intermediate_inverted_index[term.first].push_back(document_term);
    }
}

string cleanWord(
    string word
) {
    // Convert to lowercase
    transform(word.begin(), word.end(), word.begin(), ::tolower);
    // Trim leading and trailing non-alphanumeric characters (i.e., symbols) except numerals
    size_t start = 0, end = word.size();
    // Skip leading characters that are not alphabetic or numeric
    while (start < end && !isalnum(word[start])) {
        start++;
    }
    // Skip trailing characters that are not alphabetic or numeric
    while (end > start && !isalnum(word[end - 1])) {
        end--;
    }
    word = word.substr(start, end - start);
    // Remove '-' or '/' at the start or end if present
    if (!word.empty() && (word.front() == '-' || word.front() == '/' || word.back() == '-' || word.back() == '/')) {
        if (word.front() == '-' || word.front() == '/') {
            word.erase(0, 1); // Remove leading '-' or '/'
        }
        if (!word.empty() && (word.back() == '-' || word.back() == '/')) {
            word.pop_back(); // Remove trailing '-' or '/'
        }
    }
    // Check for non english characters
    if (!all_of(word.begin(), word.end(), [](char c) { return isalnum(c) || c == '-' || c == '/'; })) {
        return "";
    }
    return word;
}

void update_document_index(
    ofstream &document_index,
    string doc_id, 
    long word_count
) {
    document_index<<doc_id<<" "<<word_count<<endl;
}

int main()
{
    ifstream file("collection.tsv");
    if (!file.is_open()) {
        cerr << "Error: Could not open the file!" << endl;
        return 1;
    }
    ofstream document_index("document_index.txt");
    string line;
    map<string, vector<pair<string, long>>> intermediate_inverted_index;
    const int PASSAGES_PER_INVERTED_INDEX = 10000;
    int current_passage_index = 0;
    int intermediate_file_index = 0;
    long total_word_count = 0;
    long total_documents = 0;
    while (getline(file, line))
    {
        total_documents++;
        if (line.empty()) continue;
        istringstream line_stream(line);
        string doc_id, passage, word;
        unordered_map<string, long> terms_per_document;
        getline(line_stream, doc_id, '\t');
        getline(line_stream, passage);
        istringstream passage_stream(passage);
        long word_count = 0;
        while (passage_stream >> word)
        {
            word = cleanWord(word);
            if (word.empty()) {
                continue;
            }
            word_count++;
            terms_per_document[word] += 1;
        }
        total_word_count = total_word_count + word_count;
        update_document_index(document_index, doc_id, word_count);
        update_inverted_index(doc_id, intermediate_inverted_index, terms_per_document);
        current_passage_index++;
        if (current_passage_index >= PASSAGES_PER_INVERTED_INDEX)
        {
            write_intermediate_inverted_index(intermediate_file_index, intermediate_inverted_index);
            intermediate_inverted_index.clear();
            current_passage_index = 0;
            intermediate_file_index++;
        }
    }
    if (!intermediate_inverted_index.empty())
    {
        write_intermediate_inverted_index(intermediate_file_index, intermediate_inverted_index);
    }
    document_index<<total_word_count/total_documents<<" "<<total_documents;
    document_index.close();
    file.close();
    return 0;
}
