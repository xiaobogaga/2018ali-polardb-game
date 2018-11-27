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

static const int map_size = 1024 * 1024 * 768;

polar_race::RetCode IndexStore::init(const std::string& dir, int party) {
    this->dir_ = dir;
    this->party_ = party;

    if (!polar_race::FileExists(dir_)
        && 0 != mkdir(dir_.c_str(), 0755)) {
        fprintf(stderr, "[DataStore] : %s mkdir failed\n", dir_.c_str());
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
    }
    this->fd_ = fd;

    void* ptr = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED, this->fd_, 0);
    if (ptr == MAP_FAILED) {
        fprintf(stderr, "[IndexStore-%d] : MAP_FAILED\n", party);
        close(fd);
        return polar_race::kIOError;
    }
    if (new_create) {
     //   fprintf(stderr, "[IndexStore] : create a new mmap \n");
        memset(ptr, 0, map_size);
    }
    items_ = reinterpret_cast<Item *>(ptr);
    head_ = items_;
}

void IndexStore::add(const polar_race::PolarString& key, uint32_t info) {
    while (items_->info != 0) items_ ++;
    items_->info = info;
    memcpy(items_->key, key.data(), 8);
    items_++;
    this->size ++;
}

void IndexStore::get(const polar_race::PolarString& key, uint32_t* ans) {
    if (this->tree == NULL) initMaps();
    // radix_tree<std::string, long>::iterator ite = (*this->tree_).longest_match(key.ToString());
    // std::map<std::string, uint32_t >::iterator ite = maps->find(std::string(key.data(), 8));
    uintptr_t  ret = (uintptr_t) art_search(this->tree, (unsigned char*) key.data(), 8);
//    if (ite != maps->end()) {
//        (*ans) = ite->second;
//    } else {
//        (*ans) = 0;
//        return;
//    }
    if (ret == 0) {
        (*ans) = 0;
        return;
    } else {
        (*ans) = (uint32_t) ret;
    }

}

void IndexStore::initMaps() {
    // here we would init a radix tree from this structure.
    // this->tree_ = new radix_tree<std::string, long>(); // too slow
    // this->maps = new std::map<std::string, uint32_t>(); // consume too much memory
    this->tree = (art_tree*) malloc(sizeof(art_tree));
    art_tree_init(this->tree);
    time_t t;
    time(&t);
    struct Item* temp = items_;
    while (temp->info != 0) {
        // (*this->tree_)[std::string(temp->key, 8)] = temp->info;
        art_insert(this->tree, (unsigned char *) temp->key, 8, (void*) temp->info);
        this->size ++;
        temp++;
    }
    if (fd_ >= 0) {
        munmap(items_, map_size);
        close(fd_);
        fd_ = -1;
    }
    fprintf(stderr, "[IndexStore-%d] : init radix_tree finished, total: %d data, taken %f s\n",
            party_, this->size, difftime(time(NULL), t));
}

void IndexStore::finalize() {
    fprintf(stderr, "[IndexStore] : finalize index store with size %d\n", this->size);
    if (fd_ >= 0) {
        // items_ = NULL;
        munmap(head_, map_size);
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

    if (this->tree != NULL) {
        free(this->tree);
        this->tree = NULL;
    }

}

int visit(void *d, const unsigned char *key, uint32_t key_len, void *value) {
    void** data = (void**) d;
    uint32_t* size = (uint32_t*) data[0];
    (*size) ++;
    uint32_t v = (uint32_t) (uintptr_t) value;
    uint16_t offset = polar_race::unwrapOffset(v);
    uint16_t fileNo = polar_race::unwrapFileNo(v);
    polar_race::Visitor* visitor = (polar_race::Visitor*) data[1];
    polar_race::DataStore* store = (polar_race::DataStore*) data[2];
    std::string str;
    store->Read(fileNo, offset, &str);
    visitor->Visit(polar_race::PolarString((char*) key, 8), polar_race::PolarString(str));
    return 0;
}

int IndexStore::rangeSearch(const polar_race::PolarString& lower, const polar_race::PolarString& upper,
                 polar_race::Visitor* visitor, polar_race::DataStore* store) {
    std::string value;
    int ans = 0;
    if (this->tree == NULL) initMaps();
//    for (std::map<std::string, uint32_t >::iterator ite = maps->begin(); ite != maps->end(); ite++) {
//        uint32_t k = ite->second;
//        uint16_t offset = polar_race::unwrapOffset(k);
//        uint16_t fileNo = polar_race::unwrapFileNo(k);
//        store.Read(fileNo, offset, &value);
//        visitor.Visit(polar_race::PolarString(ite->first), polar_race::PolarString(value));
//        ans ++;
//    }
//    return ans;

    uint32_t size = 0;
    void* data[] = {&size, visitor, store};
    art_iter(this->tree, visit, &data);
    return size;
}


IndexStore::~IndexStore() {
   // fprintf(stderr, "[IndexStore] : finalize index store\n");
    if (fd_ >= 0) {
        munmap(head_, map_size);
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

    if (this->tree != NULL) {
        free(this->tree);
        this->tree = NULL;
    }

}