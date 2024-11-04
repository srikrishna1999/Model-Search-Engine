#include<iostream>
#include<sstream>
#include<fstream>
using namespace std;

ostream& operator<<(
    ostream& os, 
    const vector<unsigned long int>& vec
) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        os << vec[i];
        if (i != vec.size() - 1) {
            os << ", ";
        }
    }
    os << "]";
    return os;
}

ostream& operator<<(
    ostream& os,
    const vector<u_int8_t>& vec
) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        os << bitset<sizeof(unsigned long int)>(vec[i]);
        if (i != vec.size() - 1) {
            os << ", ";
        }
    }
    os << "]";
    return os;
}

vector<unsigned long int> mergeDocAndFreqChunks(
    vector<unsigned long int> docChunkSize,
    vector<unsigned long int> freqChunkSize
) {
    vector<unsigned long int> finalChunkList;
    for(int i=0; i<docChunkSize.size(); i++)
    {
        finalChunkList.push_back( docChunkSize[i] );
        finalChunkList.push_back( freqChunkSize[i] );
    }
    return finalChunkList;
}

vector<uint8_t> varbyte_encode(
    unsigned long int number
) {
    vector<uint8_t> encoded;
    // Continue until the number is less than 128 (fits in 7 bits)
    while (number >= 128) {
        // Add 7 bits of the number to the encoded result (MSB = 0)
        encoded.push_back(static_cast<uint8_t>(number & 127));  // Store the 7 bits
        number >>= 7;  // Shift right by 7 bits to process the next 7 bits
    }
    // Add the last 7 bits with MSB = 1 to indicate the end
    encoded.push_back(static_cast<uint8_t>(number | 128));  // Set MSB to 1
    return encoded;
}

vector<vector<uint8_t>> get_Varbyte_Encoded_List(
    vector<unsigned long int> docList,
    vector<unsigned long int> &chunkSize,
    unsigned long int &totalSize
) {
    vector<vector<uint8_t>> finalCompressedList;
    vector<uint8_t> finalList;
    unsigned long int tempSize=0UL;
    for(unsigned long int i=0; i<docList.size(); i++)
    {
        vector<uint8_t> compressed_Number_Vector= varbyte_encode(docList[i]);
        tempSize+=compressed_Number_Vector.size();
        finalList.insert(finalList.end(), compressed_Number_Vector.begin(),compressed_Number_Vector.end());
        if((i+1) % 128 == 0)
        {
            chunkSize.push_back(tempSize);
            finalCompressedList.push_back(finalList);
            totalSize+=finalList.size();
            finalList.clear();
            tempSize=0UL;
        }    
    }
    if(docList.size() % 128 != 0)
    {
        chunkSize.push_back(tempSize);
        totalSize+=finalList.size();
        finalCompressedList.push_back(finalList);
    }
    return finalCompressedList;
}

vector<unsigned long int>  update_and_remove_gaps(
    vector<unsigned long int> docList,
    vector<unsigned long int> &lastDocId
) {
    vector<unsigned long int> tempDocList;
    unsigned long int diff=0UL;
    for(unsigned long int i=0; i<docList.size(); i++)
    {
       
        if ((i+1) % 128 != 1)
        {
            tempDocList.push_back(docList[i] - docList[i-1]);
            if((i+1) % 128 == 0)
            lastDocId.push_back(docList[i]);
        }
        else{
            tempDocList.push_back(docList[i]);
        }
    }
    if(docList.size() % 128 != 0)
    lastDocId.push_back(docList[docList.size() - 1]);
    return tempDocList;
}

void store_mini_blocks_In_bin(
    ofstream& binaryFile,
    vector<vector<uint8_t>> doc_data,
    vector<vector<uint8_t>> freq_data
) {
    for(int i=0;i<doc_data.size();i++)
    {
      binaryFile.write(reinterpret_cast<const char*>(doc_data[i].data()), doc_data[i].size());
     binaryFile.write(reinterpret_cast<const char*>(freq_data[i].data()), freq_data[i].size());
    }
}

class Term {
    public:
    string term;
    string doc_frequency;
    int intermediate_file_index;

    Term(
        string term, string doc_frequency,
        int intermediate_file_index
    ) {
        this->term = term;
        this->doc_frequency = doc_frequency;
        this->intermediate_file_index = intermediate_file_index;
    }

    bool operator<(
        const Term& other
    ) const {
        if (this->term == other.term) return this->intermediate_file_index > other.intermediate_file_index;
        return this->term > other.term;
    }
};

Term split_doc_id_frequency(
    int file_index,
    string line
) {
    size_t colonPos = line.find(':');
    Term term = Term(
        line.substr(0, colonPos),
        line.substr(colonPos + 1),
        file_index
    );
    return term;
}

long get_documents_count_per_term(
    string document_id_string
) {
    std::istringstream stream(document_id_string);
    string doc_id, freq;
    long doc_count = 0;
    while (stream >> doc_id >> freq) {
        doc_count++;
    }
    return doc_count;
}

unsigned long int compress(
    string line,
    ofstream &bin_file
) {
    vector<unsigned long int> lastDocIds;
    vector<unsigned long int> chunksize;
    stringstream lineStream(line);
    string token, data;
    getline(lineStream, token, ':');
    getline(lineStream, token, ':');
    stringstream dataStream(token);
    vector<unsigned long int> docList;
    vector<unsigned long int> freqList;
    while(getline(dataStream, data, ' '))
    {
        unsigned long int data_in_num;
        istringstream(data) >> data_in_num;
        docList.push_back(data_in_num);
        getline(dataStream, data, ' ');
        istringstream(data) >> data_in_num;
        freqList.push_back(data_in_num - 1);
    }
    docList= update_and_remove_gaps(docList, lastDocIds);
    vector<unsigned long int> docIdsChunkSize;
    vector<unsigned long int> freqsChunkSize;
    unsigned long int compressed_docid_size=0UL;
    unsigned long int compressed_freq_size=0UL;
    vector<vector<uint8_t>> compressed_doc_id_block_list= get_Varbyte_Encoded_List(docList, docIdsChunkSize, compressed_docid_size);
    vector<vector<uint8_t>> compressed_freq_block_list= get_Varbyte_Encoded_List(freqList, freqsChunkSize, compressed_freq_size);
    unsigned long int lastDocIdList_size = lastDocIds.size();
    vector<unsigned long int> finalChunkSizeList = mergeDocAndFreqChunks(docIdsChunkSize, freqsChunkSize);
    unsigned long int chunkList_size = finalChunkSizeList.size();
    unsigned long int totalBlockSize = sizeof(unsigned long int) + lastDocIdList_size * sizeof(unsigned long int) + sizeof(unsigned long int) + chunkList_size * sizeof(unsigned long int) + compressed_docid_size + compressed_freq_size;
    bin_file.write(reinterpret_cast<const char*>(&totalBlockSize), sizeof(totalBlockSize));
    bin_file.write(reinterpret_cast<const char*>(&lastDocIdList_size), sizeof(lastDocIdList_size));
    bin_file.write(reinterpret_cast<const char*>(lastDocIds.data()), sizeof(unsigned long int) * lastDocIds.size());
    bin_file.write(reinterpret_cast<const char*>(&chunkList_size), sizeof(chunkList_size));
    bin_file.write(reinterpret_cast<const char*>(finalChunkSizeList.data()), sizeof(unsigned long int) * finalChunkSizeList.size());
    store_mini_blocks_In_bin(bin_file,compressed_doc_id_block_list,compressed_freq_block_list);
    return totalBlockSize;
}

int get_total_documents(ifstream& document_index) {
    document_index.seekg(0, std::ios::end);
    streampos fileSize = document_index.tellg();
    string lastLine, avg_document_size, total_documents;
    char ch;
    for (std::streamoff i = 1; i < fileSize; ++i) {
        document_index.seekg(-i, std::ios::end);
        document_index.get(ch);
        if (ch == '\n') {
            getline(document_index, lastLine);
            stringstream words(lastLine);
            words>>avg_document_size>>total_documents;
            return stoi(total_documents);
        }
    }
    return -1;
}

int main() 
{
    float BATCH_SIZE = 10000;
    unsigned long int position=0UL;
    ifstream document_index = ifstream("document_index.txt");
    int num_of_intermediate_files = ceil(get_total_documents(document_index)/BATCH_SIZE);
    ofstream inverted_index = ofstream("final_inverted_index.txt");
    ofstream lexicon = ofstream("lexicon.txt");
    ofstream bin_file("data.bin", ios::binary);
    vector<ifstream> intermediate_files(num_of_intermediate_files);
    priority_queue<Term, vector<Term>> min_heap;

    for(int i=0; i < num_of_intermediate_files; i++) {
        string file_name = "intermediate_inverted_indices/intermediate_inverted_index_" + to_string(i) + ".txt";
        intermediate_files[i].open(file_name);
    }

    for(int i=0; i < num_of_intermediate_files; i++) {
        string line;
        if (getline(intermediate_files[i], line)) {
            Term term = split_doc_id_frequency(i, line);
            min_heap.push(term);
        }
    }
    
    string prev = min_heap.top().term;
    Term prev_term = min_heap.top();
    string doc_freq_combined = "";
    long terms_processed = 0;
    long term_id = 0;
    while(!min_heap.empty()) {
        Term min_term = min_heap.top();
        min_heap.pop();
        if (min_term.term != prev) {
            long document_count_per_term = get_documents_count_per_term(doc_freq_combined);
            inverted_index<<prev<<":"<<doc_freq_combined<<endl;
            lexicon<<prev<<" "<<term_id<<" "<<document_count_per_term<<" "<<position<<endl;
            position += (sizeof(unsigned long int) + compress(prev + ":" + doc_freq_combined, bin_file));
            doc_freq_combined = min_term.doc_frequency;
            term_id++;
        }
        else if (doc_freq_combined.empty()) {
            doc_freq_combined = min_term.doc_frequency;
        }
        else doc_freq_combined = doc_freq_combined + " " + min_term.doc_frequency;
        string line;
        if (getline(intermediate_files[min_term.intermediate_file_index], line)) {
            Term term = split_doc_id_frequency(min_term.intermediate_file_index, line);
            min_heap.push(term);
        }
        else 
        {
            intermediate_files[min_term.intermediate_file_index].close();
        }
        prev = min_term.term;
        prev_term = min_term;
        terms_processed++;
        if (terms_processed % 10000 == 0) cout<<"Terms Processed : "<<terms_processed<<endl;
    }
    long document_count_per_term = get_documents_count_per_term(doc_freq_combined);
    inverted_index<<prev<<":"<<doc_freq_combined<<endl;
    lexicon<<prev<<" "<<term_id<<" "<<document_count_per_term<<" "<<position;
    position += (sizeof(unsigned long int) + compress(prev + ":" + doc_freq_combined, bin_file));
    lexicon.close();
    inverted_index.close();
    document_index.close();
    return 0;
}