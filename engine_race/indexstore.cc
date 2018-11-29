//
// Created by tomzhu on 18-11-24.
//

#include "indexstore.h"
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include "util.h"

static const int map_size = 1024 * 1024 * 12;

int compare (const void * a, const void * b)
{
    struct Info* infoA = (struct Info*) a;
    struct Info* infoB = (struct Info*) b;
    if (infoA->key == infoB->key) {
        // duplated key.
        uint16_t fileNoA = polar_race::unwrapFileNo(infoA->info);
        uint16_t fileNoB = polar_race::unwrapFileNo(infoB->info);
        if (fileNoA == fileNoB) {
            return polar_race::unwrapOffset(infoA->info) <
                   polar_race::unwrapOffset(infoB->info) ? -1 : 1;
        } else {
            return fileNoA > fileNoB ? 1 : -1;
        }

    }
    return infoA->key < infoB->key ? -1 : 1;
}

int bcompare (const void* a, const void *b) {
    long long infoA = (*(long long*) a);
    struct Info* infoB = (struct Info*) b;
    if (infoA == infoB->key) return 0;
    return infoA < infoB->key ? -1 : 1;
}

polar_race::RetCode IndexStore::init(const std::string& dir, int party) {
    this->dir_ = dir;
    this->party_ = party;

    if (!polar_race::FileExists(dir_)
        && 0 != mkdir(dir_.c_str(), 0755)) {
        fprintf(stderr, "[IndexStore-%d] : %s mkdir failed\n", party, dir_.c_str());
        return polar_race::kIOError;
    }

    this->indexPath_ = dir + indexPrefix;
    if (!polar_race::FileExists(this->indexPath_) &&
        0 != mkdir(this->indexPath_.c_str(), 0755)) {
        fprintf(stderr, "[IndexStore-%d] : mkdir failed %s\n", party, this->indexPath_.c_str());
        return polar_race::kIOError;
    }
    bool new_create = false;
    this->fileName_ = this->indexPath_ + std::to_string(party);
    int fd = open(this->fileName_.c_str(), O_RDWR, 0644);
    size_t fileLength;
    newMapSize = map_size;
    if (fd < 0 && errno == ENOENT) {
        // not exist, then create
        fd = open(this->fileName_.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd >= 0) {
            new_create = true;
            if (posix_fallocate(fd, 0, map_size) != 0) {
                fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n", party);
                close(fd);
                return polar_race::kIOError;
            }
        }
    }

    if (fd < 0) {
        fprintf(stderr, "[IndexStore-%d] : file %s open failed\n", party, this->fileName_.c_str());
        return polar_race::kIOError;
    } else {
        fileLength = polar_race::GetFileLength(this->fileName_);
        if (fileLength > newMapSize) newMapSize = fileLength;
    }
    this->fd_ = fd;

    void* ptr = mmap(NULL, newMapSize, PROT_READ | PROT_WRITE,
                     MAP_SHARED, this->fd_, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore-%d] : MAP_FAILED\n", party);
        close(fd);
        return polar_race::kIOError;
    }
    if (new_create) {
     //   fprintf(stderr, "[IndexStore] : create a new mmap \n");
        memset(ptr, 0, newMapSize);
    }
    items_ = reinterpret_cast<Item *>(ptr);
    head_ = items_;
    this->start = newMapSize;
    this->sep = newMapSize / sizeof(struct Item);
    return polar_race::RetCode::kSucc;
}

void IndexStore::reAllocate() {
    // needs reallocate.
    if (munmap(head_, newMapSize) == -1) fprintf(stderr, "[IndexStore-%d] : unmap  failed\n", party_);
    if (posix_fallocate(this->fd_, start, map_size) != 0) {
        fprintf(stderr, "[IndexStore-%d] : posix_fallocate failed\n", party_);
        close(this->fd_);
        return ;
    }
    void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED, this->fd_, this->start);
    memset(ptr, 0, map_size);
    this->start += map_size;
    this->newMapSize = map_size;
    this->sep = newMapSize / sizeof(struct Item);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore -%d] : MAP_FAILED\n", party_);
        close(this->fd_);
        return ;
    }
    items_ = reinterpret_cast<Item *>(ptr);
    head_ = items_;
}

void IndexStore::add(const polar_race::PolarString& key, uint32_t info) {
   // fprintf(stderr, "[IndexStore] : adding key %lld with info %ld\n",
   //         polar_race::strToLong(key.data()), info)

    while ((items_ - head_ < this->sep) && items_->info != 0) items_ ++;
    if (items_ - head_ >= this->sep) reAllocate();
    items_->info = info;
    memcpy(items_->key, key.data(), 8);
    items_++;
    this->size ++;
}

void IndexStore::get(long long key, uint32_t* ans) {
    if (this->infos == NULL) initMaps();
    // radix_tree<std::string, long>::iterator ite = (*this->tree_).longest_match(key.ToString());
    // std::map<std::string, uint32_t >::iterator ite = maps->find(std::string(key.data(), 8));
    // uintptr_t  ret = (uintptr_t) art_search(this->tree, (unsigned char*) key.data(), 8);
    // char buf[8];
    // polar_race::longToStr(key, buf);
    if (!this->bf->contains(key)) {
        (*ans) = 0;
        return ;
    }
    struct Info* ret = (struct Info*) bsearch(&key, this->infos, this->size,
            sizeof(struct Info), bcompare);

    // make sure it is the latest.
    while (ret != NULL && ret < this->infos + this->size && (ret + 1)->key == key) {
        ret ++;
    }
//    if (ite != maps->end()) {
//        (*ans) = ite->second;
//    } else {
//        (*ans) = 0;
//        return;
//    }



    if (ret == NULL) {
     //   fprintf(stderr, "[IndexStore] : doesn't find key %lld\n", key);
        (*ans) = 0;
        return;
    } else {
     //   fprintf(stderr, "[IndexStore] : finding key %lld with info %ld\n", key, ret->info);
        (*ans) = ret->info;
    }

}

void IndexStore::initMaps2() {
    this->table = new MyHashTable();
    fprintf(stderr, "[IndexStore-%d] : try to init map\n", party_);
    time_t t;
    time(&t);
    struct Item* temp = head_;
    this->size = 0;
    while (temp->info != 0) {
        // (*this->tree_)[std::string(temp->key, 8)] = temp->info;
        // art_insert(this->tree, (unsigned char *) temp->key, 8, (void*) temp->info);
        long long k = polar_race::strToLong(temp->key);
        this->table->add(k, temp->info);
        this->size ++;
//        if (this->size % 10000 == 0) {
//            fprintf(stderr, "[IndexStore-%d] : 10000 data added. spend %f s\n", party_, difftime(time(NULL), t));
//            time(&t);
//        }
        temp++;
    }
    if (fd_ >= 0) {
        munmap(head_, newMapSize);
        close(fd_);
        fd_ = -1;
        head_ = NULL;
    }
    fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
            party_, this->size, difftime(time(NULL), t));
}

void IndexStore::get2(long long key, uint32_t* ans) {
    if (this->table == NULL) initMaps2();
    // radix_tree<std::string, long>::iterator ite = (*this->tree_).longest_match(key.ToString());
    // std::map<std::string, uint32_t >::iterator ite = maps->find(std::string(key.data(), 8));
    // uintptr_t  ret = (uintptr_t) art_search(this->tree, (unsigned char*) key.data(), 8);
    uint32_t ret = this->table->get(key);
    (*ans) = ret;

}

void IndexStore::initMaps() {
    // here we would init a radix tree from this structure.
    //  this->tree_ = new radix_tree<std::string, long>(); // too slow
    // this->maps = new std::map<std::string, uint32_t>(); // consume too much memory
    // this->tree = (art_tree*) malloc(sizeof(art_tree));
    // art_tree_init(this->tree);
    // this->cache = new LRUCache<long long, std::string>(32768, "");
    fprintf(stderr, "[IndexStore-%d] : try to init map\n", party_);
    this->infos = (struct Info*) malloc(sizeof(struct Info) * total);
    // this->bf = new bf::basic_bloom_filter(0.0001, 1000000);

    this->bfparameters = new bloom_parameters();
    this->bfparameters->projected_element_count = 1000000;
    this->bfparameters->false_positive_probability = 0.0001; // 1 in 10000
    this->bfparameters->random_seed = 0xA5A5A5A5;
    if (!this->bfparameters)
    {
        fprintf(stderr, "[MyHashTable] : Invalid set of bloom filter parameters!\n");
        return;
    }
    this->bfparameters->compute_optimal_parameters();
    this->bf = new bloom_filter(*this->bfparameters);
    uint32_t total1 = total;
    time_t t;
    time(&t);
    struct Item* temp = head_;
    this->size = 0;
    while (temp->info != 0) {
        // (*this->tree_)[std::string(temp->key, 8)] = temp->info;
        // art_insert(this->tree, (unsigned char *) temp->key, 8, (void*) temp->info);
        if (this->size >= total1) {
            total1 *= 2;
            this->infos = (struct Info*) realloc(this->infos, sizeof(struct Info) * total1);
            if (this->infos == NULL) {
                fprintf(stderr,
                        "[IndexStore-%d] : opps try to larger infos array to %d failed\n", party_, total);
            }
        }
        this->infos[this->size].key = polar_race::strToLong(temp->key);
        this->infos[this->size].info = temp->info;
        bf->insert(this->infos[this->size].key);
        this->size ++;
        temp++;
    }
    if (fd_ >= 0) {
        munmap(head_, newMapSize);
        close(fd_);
        fd_ = -1;
        head_ = NULL;
    }
    qsort(infos, this->size, sizeof(struct Info), compare);
    fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
            party_, this->size, difftime(time(NULL), t));
}

void IndexStore::finalize() {
    fprintf(stderr, "[IndexStore] : finalize index store with size %d\n", this->size);
    if (fd_ >= 0) {
        // items_ = NULL;
        munmap(head_, newMapSize);
        items_ = NULL;
        head_ = NULL;
        close(fd_);
        fd_ = -1;
    }
    /*
    if (this->tree_ != NULL) {
        delete this->tree_;
        this->tree_ = NULL;
    }
    */
//    if (this->maps != NULL) {
//        delete this->maps;
//        this->maps = NULL;
//    }

//    if (this->tree != NULL) {
//        free(this->tree);
//        this->tree = NULL;
//    }

//    if (this->cache != NULL) {
//        delete this->cache;
//        this->cache = NULL;
//    }

    if (this->infos != NULL) {
        free(this->infos);
        this->infos = NULL;
    }

    if (this->table != NULL) {
        delete this->table;
        this->table = NULL;
    }

    if (this->bf != NULL) {
        delete this->bf;
        this->bf = NULL;
        delete this->bfparameters;
        this->bfparameters = NULL;
    }


}

int IndexStore::rangeSearch(const polar_race::PolarString& lower, const polar_race::PolarString& upper,
                 polar_race::Visitor** visitor, int vSize, polar_race::DataStore* store) {
    time_t timer;
    time(&timer);
    std::string value;
    int ans = 0;
    if (this->infos == NULL) initMaps();
    char buf[8];
    for (uint32_t i = 0; i < this->size; i++) {
        if (i + 1 < this->size && this->infos[i + 1].key == this->infos[i].key) continue;
        uint32_t k = this->infos[i].info;
        uint16_t offset = polar_race::unwrapOffset(k);
        uint16_t fileNo = polar_race::unwrapFileNo(k);
        store->Read(fileNo, offset, &value);
        polar_race::longToStr(this->infos[i].key, buf);
        polar_race::PolarString keyP(buf, 8);
        polar_race::PolarString valueP(value);
        for (int j = 0; j < vSize; j++) visitor[j]->Visit(keyP, valueP);
        ans ++;
    }
    fprintf(stderr, "[IndexStore-%d] : finished range search within %f s under %d data\n",
            party_, difftime(time(NULL), timer), ans);
    return ans;

}


IndexStore::~IndexStore() {
    fprintf(stderr, "[IndexStore-%d] : finalize index store. have saving data : %ld\n", party_, this->size);
    if (fd_ >= 0) {
        munmap(head_, newMapSize);
        close(fd_);
        fd_ = -1;
        items_ = NULL;
        head_ = NULL;
    }
    /*
    if (this->tree_ != NULL) {
        delete this->tree_;
        this->tree_ = NULL;
    }
     */

    //    if (this->maps != NULL) {
//        delete this->maps;
//        this->maps = NULL;
//    }

//    if (this->tree != NULL) {
//        free(this->tree);
//        this->tree = NULL;
//    }

    if (this->infos != NULL) {
        free(this->infos);
        this->infos = NULL;
    }

//    if (this->cache != NULL) {
//        delete this->cache;
//        this->cache = NULL;
//    }


    if (this->table != NULL) {
        delete this->table;
        this->table = NULL;
    }


    if (this->bf != NULL) {
        delete this->bf;
        this->bf = NULL;
        delete this->bfparameters;
        this->bfparameters = NULL;
    }

}