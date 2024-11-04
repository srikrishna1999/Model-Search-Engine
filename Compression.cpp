#include<iostream>
#include<fstream>
#include<string.h>
#include<sstream>

using namespace std;

// Need to store length of each document



ostream& operator<<(ostream& os, const vector<unsigned long int>& vec) {
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
ostream& operator<<(ostream& os, const vector<u_int8_t>& vec) {
    os << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        os << bitset<8>(vec[i]);
        if (i != vec.size() - 1) {
            os << ", ";
        }
    }
    os << "]";
    return os;
}

// class DataBlock_MetaData
//     {
//         public:
//         vector<int> chunk_size;
//         vector<int> last_doc_id;
//         int doc_count;
//          // Overloading the << operator for DataBlock_MetaData
//         friend ostream& operator<<(ostream& os, const DataBlock_MetaData& block) {
//             os << "Chunk Size: " << block.chunk_size;
//             os << "\nLast Block Size: " << block.last_doc_id;
//             os << "\nDoc Count: " << block.doc_count;
//             return os;
//         }
//     };
    
// class DataBlock_Data
//     {
//         vector<int> docIds;
//         vector<int> frequencies;
//     };
// class DataBlock
//     {
//         DataBlock_MetaData metadata; // Meta data for block traversal
//         vector<DataBlock_Data> datas; // Block datas containing Ids and frquencies 
//     };

// class InvertedList
//     {
//         vector<DataBlock> datablocks;
//     };

    vector<unsigned long int> mergeDocAndFreqChunks( vector<unsigned long int> docChunkSize, vector<unsigned long int> freqChunkSize  )
    {
        vector<unsigned long int> finalChunkList;
        for(int i=0;i<docChunkSize.size();i++)
        {
            finalChunkList.push_back( docChunkSize[i] );
            finalChunkList.push_back( freqChunkSize[i] );
        }
        return finalChunkList;
    }

    vector<uint8_t> varbyte_encode(unsigned long int number) 
        {
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
    vector< vector<uint8_t> > get_Varbyte_Encoded_List(vector<unsigned long int> docList,vector<unsigned long int> &chunkSize,unsigned long int &totalSize)
    {

        vector< vector<uint8_t> > finalCompressedList;
        vector<uint8_t> finalList;
        unsigned long int tempSize=0UL;
        for(unsigned long int i=0;i<docList.size();i++)
        {
            vector< uint8_t> compressed_Number_Vector= varbyte_encode(docList[i]);
            cout<<docList[i]<< " Var byte encoded size :"<< compressed_Number_Vector.size()<<endl;
            tempSize+=compressed_Number_Vector.size();
            finalList.insert(finalList.end(), compressed_Number_Vector.begin(),compressed_Number_Vector.end());
            if( (i+1) % 128 ==0 )
            {
                chunkSize.push_back(tempSize);
                finalCompressedList.push_back(finalList);
                cout<<"*"<<finalList.size()<<"*";
                totalSize+=finalList.size();
                finalList.clear();
                tempSize=0UL;
            }
            
        }
        if( docList.size()%128 !=0 )
        {
            chunkSize.push_back(tempSize);
            totalSize+=finalList.size();
            finalCompressedList.push_back(finalList);
        }
        cout<<"="<<totalSize<<"=";
        return finalCompressedList;
    }

    

    vector<unsigned long int>  update_and_remove_gaps(vector<unsigned long int> docList,vector<unsigned long int> &lastDocId)
        {
            vector<unsigned long int> tempDocList;
            unsigned long int diff=0UL;
            for(unsigned long int i=0;i<docList.size();i++)
            {
               
                if ( (i+1)%128 !=1 )
                {
                    tempDocList.push_back( docList[i]-docList[i-1] );
                    if( (i+1)%128==0 )
                    lastDocId.push_back(docList[i]);
                }
                else{
                    tempDocList.push_back(docList[i]);
                }
            }
            if(docList.size() % 128 !=0 )
            lastDocId.push_back(docList[docList.size()-1]);
            return tempDocList;
            
        }
    void store_mini_blocks_In_bin(ofstream& binaryFile,vector< vector<uint8_t> > doc_data,vector< vector<uint8_t> > freq_data)
    {
        for(int i=0;i<doc_data.size();i++)
        {
          binaryFile.write(reinterpret_cast<const char*>(doc_data[i].data()), doc_data[i].size());
         binaryFile.write(reinterpret_cast<const char*>(freq_data[i].data()), freq_data[i].size());
        }
    }

    int convert_TermDetails_To_PreCompression(string filename)
    {
        ifstream file(filename);
        string line;
        cout<<"hello";
         ofstream bin_file("data.bin", ios::binary);
        if (!bin_file.is_open()) {
            cout << "Error opening file for writing!" << endl;
            return 1;
        }
         if (!file.is_open()) {
        cerr << "Error: Could not open the file!" << endl;
        return -1;
         }   
        
                
                while(getline(file,line))
                    {
                       
                        // Example : geographic-specific :1415722 1 1415723 1 1415724 1

                        // Store the Term in lexicon
                        
                        // Intialize the MetaData block
                        vector<unsigned long int> lastDocIds;
                        vector<unsigned long int> chunksize;

                        // Parse the DocIDs and Freqs
                        stringstream lineStream(line);
                        string token,data;
                        getline(lineStream,token,':');
                        getline(lineStream,token,':');
                        stringstream dataStream(token);
                        vector<unsigned long int> docList;
                        vector<unsigned long int> freqList;
                        //cout<< token <<" "<<data;
                        while(getline(dataStream,data,' '))
                        {
                            unsigned long int data_in_num;
                            istringstream(data) >> data_in_num;
                            docList.push_back(data_in_num);
                            getline(dataStream,data,' ');
                            istringstream(data) >> data_in_num;
                            freqList.push_back(data_in_num-1);

                        }

                        docList= update_and_remove_gaps(docList,lastDocIds);

                        // Varbyte compression is done for both DocIds and freq
                        vector<unsigned long int> docIdsChunkSize;
                        vector<unsigned long int> freqsChunkSize;
                        unsigned long int compressed_docid_size=0UL;
                        unsigned long int compressed_freq_size=0UL;
                        vector< vector<uint8_t> > compressed_doc_id_block_list= get_Varbyte_Encoded_List(docList,docIdsChunkSize,compressed_docid_size);
                        vector< vector<uint8_t> > compressed_freq_block_list= get_Varbyte_Encoded_List(freqList,freqsChunkSize,compressed_freq_size);

                        unsigned long int lastDocIdList_size= lastDocIds.size() ;

                        vector<unsigned long int> finalChunkSizeList = mergeDocAndFreqChunks(docIdsChunkSize,freqsChunkSize);

                        unsigned long int chunkList_size= finalChunkSizeList.size() ;

                                                        // sizeof last doc  +  last docids size      +  size of 
                        unsigned long int totalBlockSize = 8                + lastDocIdList_size * 8 + 8 + chunkList_size * 8 + compressed_docid_size + compressed_freq_size;
                        //
                         bin_file.write(reinterpret_cast<const char*>(&totalBlockSize), sizeof(totalBlockSize));
                         bin_file.write(reinterpret_cast<const char*>(&lastDocIdList_size), sizeof(lastDocIdList_size));
                         bin_file.write(reinterpret_cast<const char*>(lastDocIds.data()), sizeof(unsigned long int) * lastDocIds.size());
                         bin_file.write(reinterpret_cast<const char*>(&chunkList_size), sizeof(chunkList_size));
                        bin_file.write(reinterpret_cast<const char*>(finalChunkSizeList.data()), sizeof(unsigned long int) * finalChunkSizeList.size());

                      store_mini_blocks_In_bin(bin_file,compressed_doc_id_block_list,compressed_freq_block_list);

                        cout << lastDocIds<<endl<<docList<<endl<<freqList;
                        cout<< lastDocIdList_size  <<" "<< chunkList_size   <<" "<< compressed_docid_size <<" "<< compressed_freq_size <<" Total size = "<<totalBlockSize<<endl;
                        
                    }
                    return 0;
            
    }

/*
    MetaData : DocId, freq

    [34,344,343] [128,128,67] : [ 30,12,32,121,2,12,4,34,34,34 ] [3,2,23,23,23,23,23,2] , [ 30,12,32,121,2,12,4,34,34,344 ] [3,2,23,23,23,23,23,2] , [ 30,12,32,121,2,12,4,34,34,343] [3,2,23,23,23,23,23,2]

    DocIDs: lengths
    34      34783473
    69      38473846
*/
 
       


int main()
{
    // DataBlock_MetaData dm1= DataBlock_MetaData();
    // dm1.chunk_size.push_back(20);
    // dm1.chunk_size.push_back(30);
    //  dm1.chunk_size.push_back(3555);
    // dm1.chunk_size.push_back(53);
    // dm1.doc_count=87687643;
    // dm1.last_doc_id.push_back(78);

    // DataBlock_MetaData dm2= DataBlock_MetaData();
    // dm2.chunk_size.push_back(20);
    // dm2.chunk_size.push_back(30);
    // dm2.doc_count=87687643;
    // dm2.last_doc_id.push_back(78);
    // dm2.last_doc_id.push_back(7835);

    // //write to bin
	// ofstream f("ClassFile.bin", ios::out | ios::app | ios::binary);
	// f.write((char*)&dm1, sizeof(dm1));
	// f.write((char*)&dm2, sizeof(dm2));
	// f.close();

	// //read back from bin
	// DataBlock_MetaData restoreBlock;
	// ifstream read("ClassFile.bin", ios::in | ios::binary);
	// while (read.is_open())
	// {
	// 	read.read((char*)&restoreBlock, sizeof(restoreBlock));
	// 	cout << restoreBlock << endl;
	// 	if (read.eof())
	// 	{
	// 		read.close();
	// 	}
    //     delete restoreBlock;
	// }


    // cout << "Hello";
   convert_TermDetails_To_PreCompression("intermediate_inverted_indices/intermediate_inverted_index_0.txt");

    
    // ofstream bin_file("data1.bin", ios::binary);
    // vector<unsigned long int> temp;
    // temp.push_back(12);
    // temp.push_back(1232323);
    // temp.push_back(12232323);
    // temp.push_back(1243434);
    // bin_file.write(reinterpret_cast<const char*>(temp.data()), temp.size()*8);
    // temp.clear();
    // cout<<temp<<endl;
    // temp.resize(4);
    // cout<<temp<<endl;
    // ifstream bin_in_file("data1.bin", ios::binary);
    // bin_in_file.read(reinterpret_cast<char*>(temp.data()), temp.size()*8);
    // cout<<temp<<endl;
    return 0;
}