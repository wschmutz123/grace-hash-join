#include <iostream>
#include <string>
#include <functional>
#include <fstream>
#include "Join.hpp"
#include "constants.hpp"
#include "Record.hpp"
#include "Join.hpp"
#include "Record.hpp"
#include "Page.hpp"
#include "Disk.hpp"
#include "Mem.hpp"
#include "Bucket.hpp"
#include "constants.hpp"
#include <algorithm>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
                         // partition:
                         // partition left
                         // loop through left_rel
                         // use hash function to put tuples into output buffers
                         // when buffers fill we push them to disk

                         // partition right
                         // same steps
                         // use same hash function

                         Disk* disk,
                         Mem* mem,
                         pair<unsigned int, unsigned int> left_rel,
                         pair<unsigned int, unsigned int> right_rel) {
    vector<Bucket> vec_buckets(MEM_SIZE_IN_PAGE-1, Bucket(disk));

    unsigned int input_buffer = MEM_SIZE_IN_PAGE-1;

    //loop through every page in left relation
    for (unsigned int pi = left_rel.first; pi < left_rel.second; pi++){
        //load page to memory from disk
        mem->loadFromDisk(disk, pi, input_buffer);
        //get page from memory we just loaded
        Page *p = mem->mem_page(input_buffer);
        //loop through every record in page
        for (unsigned int ri = 0; ri < p->size(); ri++){
            Record r = p->get_record(ri);
            //calculte partion hash for record
            unsigned int partition_hash_value = r.partition_hash();
            //pull page corresponding to partition index from memory
            Page *p2 = mem->mem_page(partition_hash_value);
            //if page is not full
            if (!p2->full()){
                //load record
                p2->loadRecord(r);
            }
            //if partition page is full
            else {

                //flush page to disk and get disk_page id
                unsigned int disk_page_id = mem->flushToDisk(disk, partition_hash_value);
                //add disk_page_id to bucket
                vec_buckets[partition_hash_value].add_left_rel_page(disk_page_id);
                //load record
                p2->loadRecord(r);
            }
        }
    }
    //when complete, loop through every page buffer and flush to disk
    for (unsigned int page_index = 0; page_index < MEM_SIZE_IN_PAGE-1; page_index++){
        if (mem->mem_page(page_index)->size() > 0){
            unsigned int disk_page_id = mem->flushToDisk(disk, page_index);
            vec_buckets[page_index].add_left_rel_page(disk_page_id);
        }
    }


    mem->reset();

    //loop through every page in right relation
    for (unsigned int pi = right_rel.first; pi < right_rel.second; pi++){
        //load page to memory from disk
        mem->loadFromDisk(disk, pi, input_buffer);
        //get page from memory we just loaded
        Page *p = mem->mem_page(input_buffer);
        //loop through every record in page
        for (unsigned int ri = 0; ri < p->size(); ri++){
            Record r = p->get_record(ri);
            //calculte partion hash for record
            unsigned int partition_hash_value = r.partition_hash();
            //pull page corresponding to partition index from memory
            Page *p2 = mem->mem_page(partition_hash_value);
            //if page is not full
            if (!p2->full()){
                //load record
                p2->loadRecord(r);
            }
            //if partition page is full
            else {

                //flush page to disk and get disk_page id
                unsigned int disk_page_id = mem->flushToDisk(disk, partition_hash_value);
                //add disk_page_id to bucket
                vec_buckets[partition_hash_value].add_right_rel_page(disk_page_id);
                //load record
                p2->loadRecord(r);
            }
        }
    }
    //when complete, loop through every page buffer and flush to disk
    for (unsigned int page_index = 0; page_index < MEM_SIZE_IN_PAGE-1; page_index++){

        if (mem->mem_page(page_index)->size() > 0){
            unsigned int disk_page_id = mem->flushToDisk(disk, page_index);
            vec_buckets[page_index].add_right_rel_page(disk_page_id);
        }
    }
    mem->reset();
    return vec_buckets;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<unsigned int> result;
    unsigned int input_buffer = MEM_SIZE_IN_PAGE - 2;
    unsigned int output_buffer = MEM_SIZE_IN_PAGE - 1;

    int left_counter = 0;
    int right_counter = 0;

    for (unsigned int i = 0; i < partitions.size(); i++){
        right_counter += partitions[i].get_right_rel().size();
        left_counter += partitions[i].get_left_rel().size();
    }

    bool left_bigger = (left_counter >= right_counter) ? true : false;

    for (unsigned int i = 0; i < partitions.size(); i++){
        vector<unsigned int> small_rel = (left_bigger) ? partitions[i].get_right_rel() : partitions[i].get_left_rel();
        vector<unsigned int> big_rel = (left_bigger) ? partitions[i].get_left_rel() : partitions[i].get_right_rel();
        //put all records of smaller relation in hash buckets
        for (unsigned int si = 0; si < small_rel.size(); si++){
            //load page to memory
            mem->loadFromDisk(disk, small_rel[si], input_buffer);
            //get page from memory
            Page *p = mem->mem_page(input_buffer);
            //loop through every record in page
            for (unsigned int ri = 0; ri < p->size(); ri++){
                Record r = p->get_record(ri);
                unsigned int probe_hash_value = r.probe_hash();
                Page *p2 = mem->mem_page(probe_hash_value);
                //if page is not full
                if (!p2->full()){
                    //load record
                    p2->loadRecord(r);
                }
                //if partition page is full
                else {
                    cerr<<"partition of smaller relation does not fit inside the in-memory hash table"<<'\n';
                }

            }
        }
        for (unsigned int bi = 0; bi < big_rel.size(); bi++){
            //load page from disk
            mem->loadFromDisk(disk, big_rel[bi], input_buffer);
            //get page from memory
            Page *input_page = mem->mem_page(input_buffer);
            Page *output_page = mem->mem_page(output_buffer);
            //loop through records in page
            for (unsigned int ri = 0; ri < input_page->size(); ri++){
                Record r = input_page->get_record(ri);
                //get hash value
                unsigned int probe_hash_value = r.probe_hash();
                Page *p = mem->mem_page(probe_hash_value);
                for (unsigned int ri2 = 0; ri2 < p->size(); ri2++){
                    if (p->get_record(ri2) == r){
                        if(!output_page->full()){
                            output_page->loadPair(p->get_record(ri2), r);
                        }
                        else{
                            unsigned int disk_page_id = mem->flushToDisk(disk, output_buffer);
                            result.push_back(disk_page_id);
                            output_page->loadPair(p->get_record(ri2), r);
                        }
                    }
                }

            }
        }
    }
    if (mem->mem_page(output_buffer)->size() > 0){
        unsigned int disk_page_id = mem->flushToDisk(disk, output_buffer);
        result.push_back(disk_page_id);
    }
    return result;
}
